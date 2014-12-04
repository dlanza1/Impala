package com.cloudera.impala.planner;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.BetweenPredicate;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.BinaryPredicate.Operator;
import com.cloudera.impala.analysis.CastExpr;
import com.cloudera.impala.analysis.CompoundPredicate;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.FunctionCallExpr;
import com.cloudera.impala.analysis.FunctionName;
import com.cloudera.impala.analysis.InPredicate;
import com.cloudera.impala.analysis.NumericLiteral;
import com.cloudera.impala.analysis.Predicate;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.analysis.StringLiteral;
import com.cloudera.impala.analysis.TupleId;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.Pair;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class PartitioningAuto {

	private final static Logger LOG = LoggerFactory
			.getLogger(PartitioningAuto.class);

	private static final String PARTITIONING_SUBSTRING = "_part_";

	private Analyzer analyzer;
	private HdfsTable tbl_;
	
	private HashMap<Integer, Pair<Expr, Expr>> betweenBounds = new HashMap<Integer, Pair<Expr, Expr>>();

	public PartitioningAuto(Analyzer analyzer, HdfsTable tbl_) {
		this.analyzer = analyzer;
		this.tbl_ = tbl_;
	}

	public void applyFilters(List<Expr> conjuncts_,
			ArrayList<TupleId> tupleIds_, List<Expr> simpleFilterConjuncts,
			List<HdfsPartitionFilter> partitionFilters)
			throws AnalysisException {
		
		long time_start = System.currentTimeMillis();
		
		if (conjuncts_.size() < 1) {
			LOG.debug("Could not be applied because there are no conjuncts to apply it.");
			return;
		}

		List<SlotId> partitionSlots = Lists.newArrayList();
		for (SlotDescriptor slotDesc : analyzer.getDescTbl().getTupleDesc(tupleIds_.get(0)).getSlots()) {
			Preconditions.checkState(slotDesc.getColumn() != null);
			if (canBeAppliedAutomaticPartitioning(slotDesc)) {
				partitionSlots.add(slotDesc.getId());
				LOG.debug("Susceptible column: "
						+ slotDesc.getColumn().getName());
			}
		}

		if (partitionSlots.size() < 1) {
			LOG.debug("Could not be applied because there are no susceptible columns to apply it.");
			return;
		}

		boolean modify = false;

		Iterator<Expr> it = conjuncts_.iterator();
		while (it.hasNext()) {
			Expr conjunct = it.next();

			LOG.debug("Trying to apply to conjunct: " + conjunct.toSql());

			if (conjunct.isBoundBySlotIds(partitionSlots)) {
				try {
					conjunct = apply((Predicate) conjunct, null);
				} catch (IllegalStateException e) {
					LOG.debug("Could not be applied to: (" + conjunct.toSql() + ") because "
							+ e.getMessage());
					e.printStackTrace();
					continue;
				} catch (Exception e) {
					LOG.debug("Could not be applied to: (" + conjunct.toSql() + ") because "
							+ e.getMessage());
					e.printStackTrace();
					continue;
				}

				if (conjunct == null)
					continue;

				modify = true;

				LOG.debug("Applied to conjunct: " + conjunct.toSql());

				if (HdfsScanNode.canEvalUsingPartitionMd(conjunct)) {
					simpleFilterConjuncts.add(Expr
							.pushNegationToOperands(conjunct));
				} else {
					HdfsPartitionFilter f = new HdfsPartitionFilter(conjunct,
							tbl_, analyzer);
					partitionFilters.add(f);
				}
			} else {
				LOG.debug("Could not be applied to: " + conjunct.toSql()
						+ " because is not fully bound by susceptible columns.");
			}
		}

		if (modify)
			analyzer.computeEquivClasses();

		LOG.debug("Time = " + (System.currentTimeMillis() - time_start) + " ms");

	}

	private Expr apply(Predicate pred, Integer between_id) throws AnalysisException {
		if (pred instanceof BinaryPredicate
				|| pred instanceof InPredicate) {
			if (pred.getBoundSlot() != null) {
				return getPredicate(pred, between_id);
			} else {
				return pred;
			}
		} else if (pred instanceof BetweenPredicate) {
			Integer b_id = betweenBounds.size();
			
			CompoundPredicate comp_pred = ((BetweenPredicate) pred).getRewrittenPredicate();
			BinaryPredicate child0 = ((BinaryPredicate) comp_pred.getChild(0));
			BinaryPredicate child1 = ((BinaryPredicate) comp_pred.getChild(1));
			
			betweenBounds.put(b_id, new Pair<Expr, Expr>(
					child0.getSlotBinding(child0.getBoundSlot().getSlotId()),
					child1.getSlotBinding(child1.getBoundSlot().getSlotId())));
			
			return apply(comp_pred, b_id);
		} else if (pred instanceof CompoundPredicate) {
			if(between_id == null)
				between_id = treatLikeBetween((CompoundPredicate) pred);

			return new CompoundPredicate(
					((CompoundPredicate) pred).getOp(),
					apply((Predicate) pred.getChild(0), between_id),
					apply((Predicate) pred.getChild(1), between_id));
		}

		throw new IllegalStateException("the " + pred.getClass().getSimpleName() + " is not compatible.");
	}
	
	private Integer treatLikeBetween(CompoundPredicate pred) {
		
		Expr child_0 = pred.getChild(0);
		Expr child_1 = pred.getChild(1);
		
		//Check if both childs are Binary predicate
		if(child_0 == null || !(child_0 instanceof BinaryPredicate)
				|| child_1 == null || !(child_1 instanceof BinaryPredicate))
			return null;
		
		BinaryPredicate bp_0 = (BinaryPredicate) child_0;
		BinaryPredicate bp_1 = (BinaryPredicate) child_1;
				
		//Check if both columns are the same 
		if(!bp_0.getBoundSlot().equals(bp_1.getBoundSlot()))
			return null;
		
		//Possibilities allowed
		// < >    > <
		// < >=   >= <
		// > <=   <= >
		// >= <=  <= >=
		Operator op_0 = bp_0.getOp();
		Operator op_1 = bp_1.getOp();
		if(!( (op_0 == Operator.LT && op_1 == Operator.GT)
				|| (op_0 == Operator.GT && op_1 == Operator.LT)
				|| (op_0 == Operator.LT && op_1 == Operator.GE) 
				|| (op_0 == Operator.GE && op_1 == Operator.LT) 
				|| (op_0 == Operator.GT && op_1 == Operator.LE) 
				|| (op_0 == Operator.LE && op_1 == Operator.GT) 
				|| (op_0 == Operator.GE && op_1 == Operator.LE) 
				|| (op_0 == Operator.LE && op_1 == Operator.GE) ))
			return null;
		
		Integer between_id = betweenBounds.size();
		betweenBounds.put(between_id, new Pair<Expr, Expr>(
				bp_0.getSlotBinding(bp_0.getBoundSlot().getSlotId()),
				bp_1.getSlotBinding(bp_1.getBoundSlot().getSlotId())));
		
		return between_id;
	}

	private Expr getPredicate(Predicate in_pred, Integer between_id) throws AnalysisException {
		Expr out_pred = null;
		
		LinkedList<SlotRef> part_columns = 
				getColumnsForAutomaticPartitioning(in_pred.getBoundSlot());
		
		//when partitioning by time and operator is different to EQ
		//we can just apply partitioning by year 
		//a wrong result would be: utc_stamp_part_month <= month('2012-07-01') AND utc_stamp_part_year <= year('2012-07-01')
		String column_name = null;
		if(part_columns.size() > 1){
			column_name = part_columns.getFirst().getColumnName();
			
			if(column_name.endsWith(PARTITIONING_SUBSTRING + "year")
						|| column_name.endsWith(PARTITIONING_SUBSTRING + "month")
						|| column_name.endsWith(PARTITIONING_SUBSTRING + "day")
						|| column_name.endsWith(PARTITIONING_SUBSTRING + "hour")){

				if(in_pred instanceof BinaryPredicate 
						&& !((BinaryPredicate) in_pred).getOp().equals(Operator.EQ))
					part_columns = getMostCoarseFilter(part_columns, between_id);
				if(in_pred instanceof InPredicate)
					part_columns = getMostCoarseFilter(part_columns, between_id);
			}else{
				throw new IllegalStateException("several partitioning columns are not compatible"
						+ " unless these functions were year, month, day and hour.");
			}
		}
		
		for (SlotRef part_column : part_columns) {
			Expr pred = null;
			if(in_pred instanceof BinaryPredicate)
				pred = getBinaryPredicate((BinaryPredicate) in_pred, part_column);
			else if(in_pred instanceof InPredicate)
				pred = getInPredicate((InPredicate) in_pred, part_column);
			
			if(out_pred == null){
				out_pred = pred;
			}else{
				out_pred = new CompoundPredicate(
						CompoundPredicate.Operator.AND, 
						pred,
						out_pred);
			}
		}
		
		return out_pred;
	}

	private LinkedList<SlotRef> getMostCoarseFilter(
			LinkedList<SlotRef> part_columns, Integer between_id) {
		
		LinkedList<SlotRef> coarseFilter = new LinkedList<SlotRef>();
		HashMap<String, SlotRef> part_columns_map = getPartitionColumnsMap(part_columns);

		if(between_id != null)
		try{
			Pair<Expr, Expr> bounds = this.betweenBounds.get(between_id);

			Calendar lower_cal = toCalendar(bounds.first);
			Calendar upper_cal = toCalendar(bounds.second);

			if(part_columns_map.containsKey("year")){
				coarseFilter.add(part_columns_map.get("year"));

				if(upper_cal.get(Calendar.YEAR) != lower_cal.get(Calendar.YEAR)) 
					return coarseFilter;
			}else 
				throw new IllegalStateException("could not be found a column for partitioning by year.");

			if(part_columns_map.containsKey("month")){
				coarseFilter.add(part_columns_map.get("month"));
				
				if(upper_cal.get(Calendar.MONTH) != lower_cal.get(Calendar.MONTH)) 
					return coarseFilter;
			}else 
				return coarseFilter;
			
			if(part_columns_map.containsKey("day")){
				coarseFilter.add(part_columns_map.get("day"));
				
				if(upper_cal.get(Calendar.DAY_OF_MONTH) != lower_cal.get(Calendar.DAY_OF_MONTH)) 
					return coarseFilter;
			}else 
				return coarseFilter;
			
			if(part_columns_map.containsKey("hour")){
				coarseFilter.add(part_columns_map.get("hour"));
				
				if(upper_cal.get(Calendar.HOUR) != lower_cal.get(Calendar.HOUR)) 
					return coarseFilter;
			}else 
				return coarseFilter;
			
		}catch(IllegalStateException e){
			LOG.debug("there was an error (" + e.getMessage() + ") when trying to obtain coarse filter for between clause,"
					+ " the existing most coarse function will be used (amoung year, month, day or hour).");
			e.printStackTrace();
		}catch(Exception e){
			LOG.debug("there was an error (" + e.getMessage() + ") when trying to obtain coarse filter for between clause,"
					+ " the existing most coarse function will be used (amoung year, month, day or hour).");
			e.printStackTrace();
		}
		
		if(coarseFilter.size() < 1){
			if(part_columns_map.containsKey("year")){
				coarseFilter.add(part_columns_map.get("year"));
				return coarseFilter;
			}
			if(part_columns_map.containsKey("month")){
				coarseFilter.add(part_columns_map.get("month"));
				return coarseFilter;
			}
			if(part_columns_map.containsKey("day")){
				coarseFilter.add(part_columns_map.get("day"));
				return coarseFilter;
			}
			if(part_columns_map.containsKey("hour")){
				coarseFilter.add(part_columns_map.get("hour"));
				return coarseFilter;
			}
		}
		
		if(coarseFilter.size() < 1)
			throw new IllegalStateException("could not be found a column for partitioning by time.");
		
		return coarseFilter;
	}

	private Calendar toCalendar(Expr expr) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("dd-M-yyyy hh:mm");

		Calendar cal = Calendar.getInstance();
		cal.setTime(sdf.parse(getDateString(expr)));
		
		return cal;
	}

	private String getDateString(Expr expr) {
		String string = null;
		
		if(expr instanceof StringLiteral){
			StringLiteral string_literal = (StringLiteral) expr;
			
			string = string_literal.getStringValue();
			
			//Add hour if it is not specified
			if(!string.contains(":"))
				string = string.concat(" 00:00");
		}else if(expr instanceof CastExpr 
				&& ((CastExpr) expr).getTargetType() == Type.TIMESTAMP){
			string = getDateString(expr.getChild(0));
		}else{
			throw new IllegalStateException("could not get the date from " 
					+ expr.getClass().getSimpleName() + " (" + expr.toSql() + ", it should be a "
					+ "cast to timestamp or a literal string with the date [and time].");
		}
		
		return string;
	}

	private HashMap<String, SlotRef> getPartitionColumnsMap(LinkedList<SlotRef> part_columns) {
		HashMap<String, SlotRef> part_columns_map = new HashMap<String, SlotRef>();
		
		for (SlotRef slotRef : part_columns)
			part_columns_map.put(
					getPartitionFunction(slotRef, null).getFnName().getFunction(), 
					slotRef);
		
		return part_columns_map;
	}

	private Expr getBinaryPredicate(BinaryPredicate bin_pred, SlotRef part_column) {
		FunctionCallExpr func = getPartitionFunction(part_column,
				bin_pred.getSlotBinding(bin_pred.getBoundSlot().getSlotId()));
		
		Operator op = getOperator(func.getFnName(), bin_pred.getOp());
		
		return new BinaryPredicate(op, part_column, func);
	}
	
	private Expr getInPredicate(InPredicate in_pred, SlotRef part_column) {
		ArrayList<Expr> inList = new ArrayList<Expr>();

		@SuppressWarnings("unchecked")
		ArrayList<Expr> actualInList = (ArrayList<Expr>) in_pred.getChildren()
				.clone();
		actualInList.remove(0);
		for (Expr expr : actualInList) {
			inList.add(getPartitionFunction(part_column, expr));
		}
		
		return new InPredicate(part_column, inList, false);
	}

	private Operator getOperator(FunctionName fnName, Operator op) {

		if (fnName.getFunction().equals("pmod")) {
			if (op != Operator.EQ)
				throw new IllegalStateException("can not be applied with "
						+ "partitioning by module and operators different than EQ.");
		}

		if (fnName.getFunction().equals("year")
				|| fnName.getFunction().equals("month")
				|| fnName.getFunction().equals("day")
				|| fnName.getFunction().equals("hour")) {
			if (op == Operator.LT)
				return Operator.LE;
			if (op == Operator.GT)
				return Operator.GE;
			if (op == Operator.NE)
				throw new IllegalStateException(
						"Autmatic partitioning can not be applied with "
								+ "partitioning by time and NE operator.");
		}

		return op;
	}

	private boolean canBeAppliedAutomaticPartitioning(SlotDescriptor slotDesc) {
		// Check if is a partitioning column
		if (slotDesc.getColumn().getPosition() < tbl_.getNumClusteringCols())
			return false;

		String subtring_part = slotDesc.getColumn().getName()
				+ PARTITIONING_SUBSTRING;

		for (Column column : tbl_.getColumns()) {
			if (column.getName().startsWith(subtring_part)) {
				return true;
			}
		}

		return false;
	}

	private FunctionCallExpr getPartitionFunction(SlotRef column, Expr binding) {
		String column_name = column.getColumnName();
		
		int index_part = column_name.lastIndexOf(PARTITIONING_SUBSTRING)
							+ PARTITIONING_SUBSTRING.length();
		int index_end_func = column_name.indexOf('_', index_part);

		String function_name = column_name.substring(
				index_part, 
				index_end_func > index_part ? index_end_func : column_name.length() );
		
		List<Expr> params = new LinkedList<Expr>();
		params.add(binding);
		
		if (function_name.equals("mod")) {
			int module = Integer.valueOf(column_name.substring(
					column_name.lastIndexOf("_") + 1, column_name.length()));

			params.add(new NumericLiteral(new BigDecimal(module)));

			function_name = "pmod";
		}

		return new FunctionCallExpr(function_name, params);
	}

	private LinkedList<SlotRef> getColumnsForAutomaticPartitioning(
			SlotRef slotRef) throws AnalysisException {

		LinkedList<SlotRef> slots_part = new LinkedList<SlotRef>();

		// Check if is a partitioning column
		if (slotRef.getDesc().getColumn().getPosition() < tbl_
				.getNumClusteringCols())
			throw new IllegalStateException("Can not be applied because "
					+ "the column (" + slotRef.getColumnName()
					+ ") is already a partitioning column.");

		String subtring_part = slotRef.getDesc().getColumn().getName()
				+ PARTITIONING_SUBSTRING;

		for (Column column : tbl_.getColumns()) {
			if (column.getName().contains(subtring_part)) {
				SlotRef part_slotRef = new SlotRef(slotRef.getTableName(),
						column.getName());

				part_slotRef.analyze(analyzer.getAnalzer(slotRef.getSlotId()));

				slots_part.add(part_slotRef);
			}
		}

		if (slots_part.size() < 1)
			throw new IllegalStateException("Can not be applied because "
					+ "the column (" + slotRef.getColumnName()
					+ ") doesn't have any column for partitioning.");

		return slots_part;
	}
}

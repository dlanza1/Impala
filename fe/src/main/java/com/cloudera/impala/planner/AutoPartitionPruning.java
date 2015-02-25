package com.cloudera.impala.planner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.CastExpr;
import com.cloudera.impala.analysis.CompoundPredicate;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.analysis.TupleId;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.Pair;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class AutoPartitionPruning {

	private final static Logger LOG = LoggerFactory
			.getLogger(AutoPartitionPruning.class);

	public static final String PARTITIONING_SUBSTRING = "_part_";

	private final Analyzer analyzer;
	private final HdfsTable tbl_;

	private static final List<String> COMPATIBLE_FUNCTIONS = Arrays.asList(
	    "mod",
	    "div",
	    "nummonths",
	    "numdays",
	    "year",
	    "month",
	    "day",
	    "hour");

	public AutoPartitionPruning(Analyzer analyzer, HdfsTable tbl_) {
		this.analyzer = analyzer;
		this.tbl_ = tbl_;
	}

	public void applyFilters(List<Expr> conjuncts_,
			ArrayList<TupleId> tupleIds_, List<Expr> simpleFilterConjuncts,
			List<HdfsPartitionFilter> partitionFilters){

		long time_start = System.currentTimeMillis();

		List<SlotId> partitionSlots = Lists.newArrayList();
		for (SlotDescriptor slotDesc : analyzer.getDescTbl().getTupleDesc(tupleIds_.get(0)).getSlots()) {
			Preconditions.checkState(slotDesc.getColumn() != null);
			if (slotDesc.getColumn().canBeAppliedAutomaticPartitionPrunning()) {
				partitionSlots.add(slotDesc.getId());
				LOG.debug("Susceptible column: " + slotDesc.getColumn().getName());
			}
		}

		boolean applied = false;

		Iterator<Expr> it = conjuncts_.iterator();
		while (it.hasNext()) {
			Expr conjunct = it.next();

			LOG.debug("Trying to apply to conjunct: " + conjunct.toSql());

			if (conjunct.isBoundBySlotIds(partitionSlots)) {
			  conjunct = apply(analyzer, tbl_, conjuncts_, conjunct);

				addFilter(simpleFilterConjuncts, partitionFilters, conjunct);
			} else {
				LOG.debug("Could not be applied to: " + conjunct.toSql()
						+ " because is not fully bound by susceptible columns.");
			}
		}

		if (applied) analyzer.computeEquivClasses();

		LOG.debug("time elapsed: " + (System.currentTimeMillis() - time_start) + " ms");
	}

  private Expr apply(Analyzer analyzer, HdfsTable tbl_,  List<Expr> conjuncts_, Expr conjunct) {
    try {
      //Look for a between predicate
      Pair<Expr, Expr> between_bounds = null;
      if(conjunct instanceof BinaryPredicate){
        Iterator<Expr> it = conjuncts_.iterator();
        while (it.hasNext() && between_bounds == null) {
          Expr other_conjunct = it.next();

          if(conjunct == other_conjunct)
            continue;

          between_bounds = new CompoundPredicate(com.cloudera.impala.analysis.CompoundPredicate.Operator.AND,
                                                      conjunct, other_conjunct).treatLikeBetween();

          if(between_bounds != null){
            LOG.info("between predicate detected with " + other_conjunct.toSql());
          }
        }
      }

      return conjunct.applyAutoPartitionPruning(analyzer, tbl_, between_bounds);
    } catch (IllegalStateException e) {
      LOG.debug("Could not be applied to: (" + conjunct.toSql() + ") because "
          + e.getMessage());
      e.printStackTrace();
      return null;
    } catch (Exception e) {
      LOG.debug("Could not be applied to: (" + conjunct.toSql() + ") because "
          + e.getMessage());
      e.printStackTrace();
      return null;
    }
  }

  private void addFilter(List<Expr> simpleFilterConjuncts,
      List<HdfsPartitionFilter> partitionFilters, Expr conjunct) {

    if(conjunct == null) return;

    try {
      if (HdfsScanNode.canEvalUsingPartitionMd(conjunct))
        simpleFilterConjuncts.add(Expr.pushNegationToOperands(conjunct));
      else
        partitionFilters.add(new HdfsPartitionFilter(conjunct,tbl_, analyzer));

      LOG.debug("Filter created: " + conjunct.toSql());
    } catch (IllegalStateException e) {
      LOG.debug("there was an error creating the filter to: (" + conjunct.toSql() + ") because "
          + e.getMessage());
      e.printStackTrace();
    } catch (Exception e) {
      LOG.debug("there was an error creating the filter to: (" + conjunct.toSql() + ") because "
          + e.getMessage());
      e.printStackTrace();
    }

  }

  /**
   * Get the list of valid partition columns when the partitioning is by time
   *
   * @param part_columns All partitioning columns that correspond to one column
   * @param between_bounds Bounds if the parent is a BetweenPredicate
   * @return Valid partitioning columns
   * @throws AnalysisException
   */
  public static LinkedList<SlotRef> getPartitioningColumnsForTime(
      LinkedList<SlotRef> part_columns, Pair<Expr, Expr> between_bounds) throws AnalysisException {

    LinkedList<SlotRef> valid_part_columns = new LinkedList<SlotRef>();
    HashMap<String, SlotRef> part_columns_map = getPartitionColumnsMap(part_columns);

    if(between_bounds != null)
    try{

      Calendar lower_cal = ((CastExpr) between_bounds.first).toCalendar();
      Calendar upper_cal = ((CastExpr) between_bounds.second).toCalendar();

      if(part_columns_map.containsKey("year")){
        valid_part_columns.add(part_columns_map.get("year"));

        if(upper_cal.get(Calendar.YEAR) != lower_cal.get(Calendar.YEAR))
          return valid_part_columns;
      }else
        throw new IllegalStateException("could not be found a column for partitioning by year.");

      if(part_columns_map.containsKey("month")){
        valid_part_columns.add(part_columns_map.get("month"));

        if(upper_cal.get(Calendar.MONTH) != lower_cal.get(Calendar.MONTH))
          return valid_part_columns;
      }else
        return valid_part_columns;

      if(part_columns_map.containsKey("day")){
        valid_part_columns.add(part_columns_map.get("day"));

        if(upper_cal.get(Calendar.DAY_OF_MONTH) != lower_cal.get(Calendar.DAY_OF_MONTH))
          return valid_part_columns;
      }else
        return valid_part_columns;

      if(part_columns_map.containsKey("hour")){
        valid_part_columns.add(part_columns_map.get("hour"));

        if(upper_cal.get(Calendar.HOUR) != lower_cal.get(Calendar.HOUR))
          return valid_part_columns;
      }else
        return valid_part_columns;

    }catch(IllegalStateException e){
      LOG.debug("There was an error (" + e.getMessage() + ") when trying "
          + "to obtain partition columns for between clause,"
          + " the existing most coarse function will be used (amoung year, month, day or hour).");
      e.printStackTrace();
    }catch(Exception e){
      LOG.debug("There was an error (" + e.getMessage() + ") when trying "
          + "to obtain partition columns for between clause,"
          + " the existing most coarse function will be used (amoung year, month, day or hour).");
      e.printStackTrace();
    }

    if(valid_part_columns.size() < 1 && part_columns_map.containsKey("year"))
      valid_part_columns.add(part_columns_map.get("year"));

    if(valid_part_columns.size() < 1)
      throw new IllegalStateException("could not be found any column for partitioning by time.");

    return valid_part_columns;
  }

  /**
   * Convert a list of partitioning columns into a HasMap where the key is
   * the correspond partition function and the value is the partitioning column
   *
   * @param part_columns List of partitioning columns to convert
   * @return HashMap where the key is the correspond partition
   * function and the value is the partitioning column
   * @throws AnalysisException
   */
  private static HashMap<String, SlotRef> getPartitionColumnsMap(LinkedList<SlotRef> part_columns) throws AnalysisException {
    HashMap<String, SlotRef> part_columns_map = new HashMap<String, SlotRef>();

    for (SlotRef slotRef : part_columns){
      part_columns_map.put(
          slotRef.getPartitionFunction(null).getFnName().getFunction(),
          slotRef);
    }

    return part_columns_map;
  }

  /**
   * Check if the function is compatible
   *
   * @param function_name Name of the function to check
   * @throws AnalysisException If the function is not compatible
   */
  public static void checkFunctionCompatibility(String function_name) throws AnalysisException {
    if(!COMPATIBLE_FUNCTIONS.contains(function_name.toLowerCase()))
      throw new AnalysisException("the function " + function_name + " is not compatible");
  }

}

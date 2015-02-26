package com.cloudera.impala.catalog;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.ArithmeticExpr;
import com.cloudera.impala.analysis.ArithmeticExpr.Operator;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.FunctionCallExpr;
import com.cloudera.impala.analysis.NumericLiteral;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.planner.AutoPartitionPruningHelper;

/**
 * Represents columns that can be used to prune partitions but
 * it is not a partitioning (clustering) column.
 */
public class VirtualColumn extends Column {

  public static final String SUBSTRING = "_part_";

  /**
   * Column in which applies
   */
  protected Column applies;

  public VirtualColumn(String name, Type type, int position) {
    this(name, type, null, position);
  }

  public VirtualColumn(String name, Type type, String comment, int pos) {
    super(name, type, comment, pos);
  }

  public String getColumnNameInWhichApplies() {
    return name_.substring(0, name_.indexOf(SUBSTRING));
  }

  public void setColumnToBeApplied(Column applies) throws TableLoadingException{

    if(this.applies != null)
      throw new TableLoadingException("the column to be applied is already set");

    applies.addApplicableColumn(this);
    this.applies = applies;
  }

  @Override
  public boolean canBeAppliedAutomaticPartitionPrunning() {
    return false;
  }

  @Override
  public void addApplicableColumn(VirtualColumn virtual_col) throws TableLoadingException {
    throw new TableLoadingException("a virtual column (" + name_
        + ") cannot be applied to other virtual column" + virtual_col.name_);
  }

  public Column getColumnInWhichApplies() {
    return applies;
  }

  public SlotRef newSlotRef(Analyzer analyzer) throws AnalysisException {
    SlotRef slotRef = new SlotRef(null, name_);

    slotRef.analyze(analyzer);
    analyzer.createIdentityEquivClasses();

    return slotRef;
  }

  public FunctionCallExpr getPartitionFunction(Expr binding) throws AnalysisException {
    int index_part = name_.lastIndexOf(VirtualColumn.SUBSTRING)
              + VirtualColumn.SUBSTRING.length();
    int index_end_func = name_.indexOf('_', index_part);

    String function_name = name_.substring(
        index_part,
        index_end_func > index_part ? index_end_func : name_.length() );

    AutoPartitionPruningHelper.checkFunctionCompatibility(function_name);

    List<Expr> params = new LinkedList<Expr>();
    params.add(binding);

    if (function_name.equals("mod")) {
      int module = Integer.valueOf(name_.substring(
          name_.lastIndexOf("_") + 1, name_.length()));

      params.add(new NumericLiteral(new BigDecimal(module)));

      function_name = "pmod";
    }else if(function_name.equals("div")){
      int divisor = Integer.valueOf(name_.substring(
          name_.lastIndexOf("_") + 1, name_.length()));

      ArithmeticExpr arit = new ArithmeticExpr(Operator.DIVIDE,
                                              binding,
                                              new NumericLiteral(new BigDecimal(divisor)));
      params.remove(binding);
      params.add(arit);

      function_name = "floor";
    }else if(function_name.equals("nummonths")){
      //year('2012-08-17 05:14:43') * 12 + month('2012-08-17 05:14:43')
      FunctionCallExpr year = new FunctionCallExpr("year", Arrays.asList(binding));
      FunctionCallExpr month = new FunctionCallExpr("month", Arrays.asList(binding));

      ArithmeticExpr arit = new ArithmeticExpr(Operator.ADD,
          new ArithmeticExpr(Operator.MULTIPLY, year, new NumericLiteral(new BigDecimal(12))),
          month);

      params.remove(binding);
      params.add(arit);

      function_name = "floor";
    }else if(function_name.equals("numdays")){
      FunctionCallExpr year = new FunctionCallExpr("year", Arrays.asList(binding));
      FunctionCallExpr day_of_year = new FunctionCallExpr("dayofyear", Arrays.asList(binding));

      ArithmeticExpr arit = new ArithmeticExpr(Operator.ADD,
          new ArithmeticExpr(Operator.MULTIPLY, year, new NumericLiteral(new BigDecimal(365))),
          day_of_year);

      params.remove(binding);
      params.add(arit);

      function_name = "floor";
    }

    return new FunctionCallExpr(function_name, params);
  }
}

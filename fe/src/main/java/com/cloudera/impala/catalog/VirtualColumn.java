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

  private static final List<String> COMPATIBLE_FUNCTIONS = Arrays.asList(
      "mod",
      "div",
      "nummonths",
      "numdays",
      "year",
      "month",
      "day",
      "hour");

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

  public FunctionCallExpr getFunction(Expr binding) throws AnalysisException {
    String function_name = getFunctionName();

    List<Expr> params = new LinkedList<Expr>();

    if (function_name.equals("mod")) {
      int module = Integer.valueOf(name_.substring(
          name_.lastIndexOf("_") + 1, name_.length()));

      params.add(binding);
      params.add(new NumericLiteral(new BigDecimal(module)));

      function_name = "pmod";
    }else if(function_name.equals("div")){
      int divisor = Integer.valueOf(name_.substring(
          name_.lastIndexOf("_") + 1, name_.length()));

      params.add(new ArithmeticExpr(Operator.DIVIDE,
                                    binding,
                                    new NumericLiteral(new BigDecimal(divisor))));

      function_name = "floor";
    }else if(function_name.equals("nummonths")){
      //year('2012-08-17 05:14:43') * 12 + month('2012-08-17 05:14:43')
      FunctionCallExpr year = new FunctionCallExpr("year", Arrays.asList(binding));
      FunctionCallExpr month = new FunctionCallExpr("month", Arrays.asList(binding));

      params.add(new ArithmeticExpr(Operator.ADD,
          new ArithmeticExpr(Operator.MULTIPLY, year, new NumericLiteral(new BigDecimal(12))),
          month));

      function_name = "floor";
    }else if(function_name.equals("numdays")){
      FunctionCallExpr year = new FunctionCallExpr("year", Arrays.asList(binding));
      FunctionCallExpr day_of_year = new FunctionCallExpr("dayofyear", Arrays.asList(binding));

      params.add(new ArithmeticExpr(Operator.ADD,
          new ArithmeticExpr(Operator.MULTIPLY, year, new NumericLiteral(new BigDecimal(365))),
          day_of_year));

      function_name = "floor";
    }

    return new FunctionCallExpr(function_name, params);
  }

  private String getFunctionName() throws AnalysisException {
    int index_part = name_.lastIndexOf(VirtualColumn.SUBSTRING)
        + VirtualColumn.SUBSTRING.length();
    int index_end_func = name_.indexOf('_', index_part);

    String function_name = name_.substring(index_part,
      index_end_func > index_part ? index_end_func : name_.length() );

    checkFunctionCompatibility(function_name);

    return function_name;
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

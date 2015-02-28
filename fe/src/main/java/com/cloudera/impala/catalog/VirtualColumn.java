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
import com.cloudera.impala.common.Pair;

/**
 * Represents columns that can be used to prune partitions but
 * it is not a partitioning (clustering) column.
 */
public class VirtualColumn extends Column {

  final FUNCTION function;

  enum FUNCTION {
    YEAR {
      @Override
      void setFunctionName() {
        function_name = "year";
      }
    },
    MONTH {
      @Override
      void setFunctionName() {
        function_name = "month";
      }
    },
    DAY {
      @Override
      void setFunctionName() {
        function_name = "day";
      }
    },
    HOUR {
      @Override
      void setFunctionName() {
        function_name = "hour";
      }
    },
    NUMMONTHS {
      @Override
      void setFunctionName() {
        function_name = "floor";
      }

      @Override
      public FunctionCallExpr get(Expr binding) {
        // year('2012-08-17 05:14:43') * 12 + month('2012-08-17 05:14:43')
        FunctionCallExpr year = new FunctionCallExpr("year", Arrays.asList(binding));
        FunctionCallExpr month = new FunctionCallExpr("month", Arrays.asList(binding));

        List<Expr> params_func = new LinkedList<Expr>();
        params_func.add(new ArithmeticExpr(Operator.ADD,
            new ArithmeticExpr(Operator.MULTIPLY, year, new NumericLiteral(new BigDecimal(12))),
            month));

        return new FunctionCallExpr(function_name, params_func);
      }
    },
    NUMDAYS {
      @Override
      void setFunctionName() {
        function_name = "floor";
      }

      @Override
      public FunctionCallExpr get(Expr binding) {
        FunctionCallExpr year = new FunctionCallExpr("year", Arrays.asList(binding));
        FunctionCallExpr day_of_year = new FunctionCallExpr("dayofyear", Arrays.asList(binding));

        List<Expr> params_func = new LinkedList<Expr>();
        params_func.add(new ArithmeticExpr(Operator.ADD,
            new ArithmeticExpr(Operator.MULTIPLY, year, new NumericLiteral(new BigDecimal(365))),
            day_of_year));

        return new FunctionCallExpr(function_name, params_func);
      }
    },
    MOD {
      @Override
      void setFunctionName() {
        function_name = "pmod";
      }

      @Override
      void computeArguments(String params_s) throws AnalysisException {
        if(params_s.length() == 0)
          throw new AnalysisException("the function " + toString()
              + " must have one param which indicates the module to use");

        if(params_s.charAt(0) != '_')
          throw new AnalysisException("after the name of the function "
              + toString() + " must be a underscore (_), not " + params_s.charAt(0));

        String module_s = params_s.substring(1, params_s.length());
        int module;
        try{
          module = Integer.valueOf(module_s);
        }catch(NumberFormatException e){
          throw new AnalysisException("the argument (" + module_s +") for " + toString()
              + " should be a number");
        }

        args = new Expr[1];
        args[0] = new NumericLiteral(new BigDecimal(module));
      }

      @Override
      public FunctionCallExpr get(Expr binding) {
        List<Expr> params_func = new LinkedList<Expr>();
        params_func.add(binding);
        params_func.add(args[0]);

        return new FunctionCallExpr(function_name, params_func);
      }
    },
    DIV {
      @Override
      void setFunctionName() {
        function_name = "div";
      }

      @Override
      void computeArguments(String args_s) throws AnalysisException {
        if(args_s.length() == 0)
          throw new AnalysisException("the function " + toString()
              + " must have one param which indicates the divisor to use");

        if(args_s.charAt(0) != '_')
          throw new AnalysisException("after the name of the function "
              + toString() + " must be a underscore (_), not " + args_s.charAt(0));

        String div_s = args_s.substring(1, args_s.length());
        int div;
        try{
          div = Integer.valueOf(div_s);
        }catch(NumberFormatException e){
          throw new AnalysisException("the argument (" + div_s +") for " + toString()
              + " should be a number");
        }

        args = new Expr[1];
        args[0] = new NumericLiteral(new BigDecimal(div));
      }

      @Override
      public FunctionCallExpr get(Expr binding) {
        List<Expr> params = new LinkedList<Expr>();
        params.add(new ArithmeticExpr(Operator.DIVIDE, binding, args[0]));

        return new FunctionCallExpr(function_name, params);
      }
    };

    FUNCTION() {
      setFunctionName();
    }

    protected Expr[] args;
    protected String function_name;

    abstract void setFunctionName();

    public FunctionCallExpr get(Expr binding) {
      List<Expr> params = new LinkedList<Expr>();
      params.add(binding);

      return new FunctionCallExpr(function_name, params);
    }

    void computeArguments(String args_s) throws AnalysisException {
      if (args_s.length() > 0)
        throw new AnalysisException("the funcion " + toString()
            + " can not received arguments (" + args_s + ")");
    }
  }

  public static final String SUBSTRING = "_part_";

  /**
   * Column in which applies
   */
  protected Column applies;

  public VirtualColumn(String name, Type type, int position) throws AnalysisException {
    this(name, type, null, position);
  }

  public VirtualColumn(String name, Type type, String comment, int pos) throws AnalysisException {
    super(name, type, comment, pos);

    int index_substring = name.indexOf(SUBSTRING);
    if (index_substring == -1)
      throw new AnalysisException("the name of the virtual column (" +
          name + ") should contain " + SUBSTRING);

    int index_start = index_substring + SUBSTRING.length();
    int index_end = name.indexOf("_", index_start);
    if (index_end == -1)
      index_end = name.length();

    if (index_start == index_end)
      throw new AnalysisException("the name of the virtual column (" +
                        name + ") should contain the name of the function after " + SUBSTRING);

    String function_name = name.substring(index_start, index_end);

    try {
      function = FUNCTION.valueOf(function_name.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new AnalysisException("the function (" + function_name
          + ") is not compatible with virtual columns");
    }

    function.computeArguments(name.substring(index_end, name.length()));
  }

  public String getColumnNameInWhichApplies() {
    return name_.substring(0, name_.indexOf(SUBSTRING));
  }

  public void setColumnToBeApplied(Column applies) throws TableLoadingException {

    if (this.applies != null)
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

  public FunctionCallExpr getFunction(Expr slotBinding) {
    return function.get(slotBinding);
  }

  @Override
  public LinkedList<VirtualColumn> getAplicableColumns(Pair<Expr, Expr> between_bounds)
      throws AnalysisException {
    throw new AnalysisException("over virtual columns can not apply other columns");
  }

}

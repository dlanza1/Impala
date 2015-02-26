package com.cloudera.impala.planner;

import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.CastExpr;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.catalog.VirtualColumn;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.Pair;

public class AutoPartitionPruningHelper {

	private final static Logger LOG = LoggerFactory
			.getLogger(AutoPartitionPruningHelper.class);

	private static final List<String> COMPATIBLE_FUNCTIONS = Arrays.asList(
	    "mod",
	    "div",
	    "nummonths",
	    "numdays",
	    "year",
	    "month",
	    "day",
	    "hour");

  /**
   * Get the list of valid partition columns when the partitioning
   * use no monotonic time functions
   *
   * @param part_columns All partitioning columns that correspond to one column
   * @param between_bounds Bounds if the parent is a BetweenPredicate
   * @return Valid partitioning columns
   * @throws AnalysisException
   */
  public static LinkedList<VirtualColumn> getPartitioningColumnsForTime(
      LinkedList<VirtualColumn> virtual_columns, Pair<Expr, Expr> between_bounds) throws AnalysisException {

    LinkedList<VirtualColumn> valid_part_columns = new LinkedList<VirtualColumn>();
    HashMap<String, VirtualColumn> part_columns_map = getPartitionColumnsMap(virtual_columns);

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
  private static HashMap<String, VirtualColumn> getPartitionColumnsMap(LinkedList<VirtualColumn> virtual_columns)
      throws AnalysisException {
    HashMap<String, VirtualColumn> virtual_columns_map = new HashMap<String, VirtualColumn>();

    for (VirtualColumn virtualColumn : virtual_columns){
      virtual_columns_map.put(
          virtualColumn.getPartitionFunction(null).getFnName().getFunction(),
          virtualColumn);
    }

    return virtual_columns_map;
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

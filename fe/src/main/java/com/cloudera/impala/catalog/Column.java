// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.catalog;

import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.CastExpr;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.thrift.TColumn;
import com.cloudera.impala.thrift.TColumnStats;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Internal representation of column-related metadata.
 * Owned by Catalog instance.
 */
public class Column {
  private final static Logger LOG = LoggerFactory.getLogger(Column.class);

  protected final String name_;
  protected final Type type_;
  protected final String comment_;
  protected int position_;  // in table
  protected final ColumnStats stats_;

  /**
   * If this column is used for automatic partition
   * pruning as a column that can prune partitions,
   * these ones are the virtual columns that can be applied.
   */
  protected LinkedList<VirtualColumn> aplicable_columns;

  protected Column(String name, Type type, int position) {
    this(name, type, null, position);
  }

  protected Column(String name, Type type, String comment, int position) {
    name_ = name;
    type_ = type;
    comment_ = comment;
    position_ = position;
    stats_ = new ColumnStats(type);

    aplicable_columns = null;
  }

  public static Column create(String name, Type type, String comment, int pos) {
    int index_part_subs = name.indexOf(VirtualColumn.SUBSTRING);

    boolean virtual = index_part_subs > 0
        && (index_part_subs + VirtualColumn.SUBSTRING.length()) < name.length();

    if(virtual){
      try {
        return new VirtualColumn(name, type, comment, pos);
      } catch (AnalysisException e) {
        e.printStackTrace();
        return new Column(name, type, comment, pos);
      }
    }else{
      return new Column(name, type, comment, pos);
    }
  }

  public String getComment() { return comment_; }
  public String getName() { return name_; }
  public Type getType() { return type_; }
  public int getPosition() { return position_; }
  public void setPosition(int position) { this.position_ = position; }
  public ColumnStats getStats() { return stats_; }

  public boolean updateStats(ColumnStatisticsData statsData) {
    boolean statsDataCompatibleWithColType = stats_.update(type_, statsData);
    LOG.debug("col stats: " + name_ + " #distinct=" + stats_.getNumDistinctValues());
    return statsDataCompatibleWithColType;
  }

  public void updateStats(TColumnStats statsData) {
    stats_.update(type_, statsData);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this.getClass())
                  .add("name_", name_)
                  .add("type_", type_)
                  .add("comment_", comment_)
                  .add("stats", stats_)
                  .add("position_", position_).toString();
  }

  public static Column fromThrift(TColumn columnDesc) {
    String comment = columnDesc.isSetComment() ? columnDesc.getComment() : null;
    Preconditions.checkState(columnDesc.isSetPosition());
    int position = columnDesc.getPosition();
    Column col;
    if (columnDesc.isIs_hbase_column()) {
      // HBase table column. The HBase column qualifier (column name) is not be set for
      // the HBase row key, so it being set in the thrift struct is not a precondition.
      Preconditions.checkState(columnDesc.isSetColumn_family());
      Preconditions.checkState(columnDesc.isSetIs_binary());
      col = new HBaseColumn(columnDesc.getColumnName(), columnDesc.getColumn_family(),
          columnDesc.getColumn_qualifier(), columnDesc.isIs_binary(),
          Type.fromThrift(columnDesc.getColumnType()), comment, position);
    } else {
      // Hdfs table column.
      col = Column.create(columnDesc.getColumnName(),
          Type.fromThrift(columnDesc.getColumnType()), comment, position);
    }
    if (columnDesc.isSetCol_stats()) col.updateStats(columnDesc.getCol_stats());
    return col;
  }

  public TColumn toThrift() {
    TColumn colDesc = new TColumn(name_, type_.toThrift());
    if (comment_ != null) colDesc.setComment(comment_);
    colDesc.setPosition(position_);
    colDesc.setCol_stats(getStats().toThrift());
    return colDesc;
  }

  /**
   * Check if automatic partition pruning can be applied
   * with this column
   *
   * @return True if can be used for this purpose, otherwise false
   */
  public boolean canBeAppliedAutomaticPartitionPrunning() {
    return aplicable_columns != null && aplicable_columns.size() > 0;
  }

  protected void addApplicableColumn(VirtualColumn virtual_col) throws TableLoadingException {
    if(aplicable_columns == null)
      aplicable_columns = new LinkedList<VirtualColumn>();

    aplicable_columns.add(virtual_col);
  }

  public LinkedList<VirtualColumn> getAplicableColumns(Pair<Expr, Expr> between_bounds) throws AnalysisException{

    if(aplicable_columns == null)
      throw new AnalysisException("there is no aplicable columns for " + name_);

    if(aplicable_columns.size() == 1 || useMonotonicFunctions())
      return aplicable_columns;

    LinkedList<VirtualColumn> valid_part_columns = new LinkedList<VirtualColumn>();
    HashMap<String, VirtualColumn> part_columns_map = getPartitionColumnsMap(aplicable_columns);

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
          virtualColumn.getFunction(null).getFnName().getFunction(),
          virtualColumn);
    }

    return virtual_columns_map;
  }

  private boolean useMonotonicFunctions(){
    for (VirtualColumn virtual_column : aplicable_columns) {
      String virtual_column_name = virtual_column.getName();

      if(virtual_column_name.endsWith(VirtualColumn.SUBSTRING + "year")
            || virtual_column_name.endsWith(VirtualColumn.SUBSTRING + "month")
            || virtual_column_name.endsWith(VirtualColumn.SUBSTRING + "day")
            || virtual_column_name.endsWith(VirtualColumn.SUBSTRING + "hour")){
        return false;
      }
    }

    return true;
  }

  public LinkedList<VirtualColumn> getAplicableColumns() {
    return aplicable_columns;
  }

}

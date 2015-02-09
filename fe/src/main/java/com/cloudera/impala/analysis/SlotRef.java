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

package com.cloudera.impala.analysis;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.cloudera.impala.analysis.ArithmeticExpr.Operator;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.planner.AutoPartitionPruning;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TSlotRef;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class SlotRef extends Expr {

  private final TableName tblName_;
  private final String col_;
  private final String label_;  // printed in toSql()

  // results of analysis
  private SlotDescriptor desc_;

  public SlotDescriptor getDesc() {
    Preconditions.checkState(isAnalyzed_);
    Preconditions.checkNotNull(desc_);
    return desc_;
  }

  public SlotId getSlotId() {
    Preconditions.checkState(isAnalyzed_);
    Preconditions.checkNotNull(desc_);
    return desc_.getId();
  }

  public SlotRef(TableName tblName, String col) {
    super();
    this.tblName_ = tblName;
    this.col_ = col;
    this.label_ = ToSqlUtils.getIdentSql(col);
  }

  // C'tor for a "pre-analyzed" ref to a slot
  public SlotRef(SlotDescriptor desc) {
    super();
    this.tblName_ = null;
    if (desc.getColumn() != null) {
      this.col_ = desc.getColumn().getName();
    } else {
      this.col_ = null;
    }
    this.isAnalyzed_ = true;
    this.desc_ = desc;
    this.type_ = desc.getType();
    String alias = desc.getParent().getAlias();
    //this.label =  desc.getLabel();
    // TODO: should this simply be the SlotDescriptor's label?
    this.label_ = (alias != null ? alias + "." : "") + desc.getLabel();
    this.numDistinctValues_ = desc.getStats().getNumDistinctValues();
  }

  /**
   * C'tor for cloning.
   */
  private SlotRef(SlotRef other) {
    super(other);
    tblName_ = other.tblName_;
    col_ = other.col_;
    label_ = other.label_;
    desc_ = other.desc_;
    type_ = other.type_;
    isAnalyzed_ = other.isAnalyzed_;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;
    super.analyze(analyzer);
    desc_ = analyzer.registerColumnRef(tblName_, col_);
    type_ = desc_.getType();
    if (!type_.isSupported()) {
      throw new AnalysisException("Unsupported type '"
          + type_.toString() + "' in '" + toSql() + "'.");
    }
    if (type_.isInvalid()) {
      // In this case, the metastore contained a string we can't parse at all
      // e.g. map. We could report a better error if we stored the original
      // HMS string.
      throw new AnalysisException("Unsupported type in '" + toSql() + "'.");
    }
    type_.analyze();
    numDistinctValues_ = desc_.getStats().getNumDistinctValues();
    if (type_.isBoolean()) selectivity_ = DEFAULT_SELECTIVITY;
    isAnalyzed_ = true;
  }

  @Override
  public String toSqlImpl() {
    if (tblName_ != null) {
      Preconditions.checkNotNull(label_);
      return tblName_.toSql() + "." + label_;
    } else if (label_ == null) {
      return "<slot " + Integer.toString(desc_.getId().asInt()) + ">";
    } else {
      return label_;
    }
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.SLOT_REF;
    msg.slot_ref = new TSlotRef(desc_.getId().asInt());
    Preconditions.checkState(desc_.getParent().isMaterialized(),
        String.format("Illegal reference to non-materialized tuple: tid=%s",
            desc_.getParent().getId()));
    // we shouldn't be sending exprs over non-materialized slots
    Preconditions.checkState(desc_.isMaterialized(),
        String.format("Illegal reference to non-materialized slot: tid=%s sid=%s",
            desc_.getParent().getId(), desc_.getId()));
    // we also shouldn't have forgotten to compute the mem layout
    Preconditions.checkState(desc_.getByteOffset() != -1,
        String.format("Missing memory layout for tuple with tid=%s",
            desc_.getParent().getId()));
  }

  @Override
  public String debugString() {
    Objects.ToStringHelper toStrHelper = Objects.toStringHelper(this);
    String tblNameStr = (tblName_ == null ? "null" : tblName_.toString());
    toStrHelper.add("tblName", tblNameStr);
    toStrHelper.add("type", type_);
    toStrHelper.add("col", col_);
    String idStr = (desc_ == null ? "null" : Integer.toString(desc_.getId().asInt()));
    toStrHelper.add("id", idStr);
    return toStrHelper.toString();
  }

  @Override
  public int hashCode() {
    if (desc_ != null) return desc_.getId().hashCode();
    return Objects.hashCode(tblName_, (col_ == null) ? null : col_.toLowerCase());
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) return false;
    SlotRef other = (SlotRef) obj;
    // check slot ids first; if they're both set we only need to compare those
    // (regardless of how the ref was constructed)
    if (desc_ != null && other.desc_ != null) {
      return desc_.getId().equals(other.desc_.getId());
    }
    if ((tblName_ == null) != (other.tblName_ == null)) return false;
    if (tblName_ != null && !tblName_.equals(other.tblName_)) return false;
    if ((col_ == null) != (other.col_ == null)) return false;
    if (col_ != null && !col_.toLowerCase().equals(other.col_.toLowerCase())) return false;
    return true;
  }

  @Override
  public boolean isBoundByTupleIds(List<TupleId> tids) {
    Preconditions.checkState(desc_ != null);
    for (TupleId tid: tids) {
      if (tid.equals(desc_.getParent().getId())) return true;
    }
    return false;
  }

  @Override
  public boolean isBoundBySlotIds(List<SlotId> slotIds) {
    Preconditions.checkState(isAnalyzed_);
    return slotIds.contains(desc_.getId());
  }

  @Override
  public void getIdsHelper(Set<TupleId> tupleIds, Set<SlotId> slotIds) {
    Preconditions.checkState(type_.isValid());
    Preconditions.checkState(desc_ != null);
    if (slotIds != null) slotIds.add(desc_.getId());
    if (tupleIds != null) tupleIds.add(desc_.getParent().getId());
  }

  public String getColumnName() { return col_; }

  @Override
  public Expr clone() { return new SlotRef(this); }

  @Override
  public String toString() {
    if (desc_ != null) {
      return "tid=" + desc_.getParent().getId() + " sid=" + desc_.getId();
    }
    return "no desc set";
  }

  @Override
  protected Expr uncheckedCastTo(Type targetType) throws AnalysisException {
    if (type_.isNull()) {
      // Hack to prevent null SlotRefs in the BE
      return NullLiteral.create(targetType);
    } else {
      return super.uncheckedCastTo(targetType);
    }
  }

  public TableName getTableName() {
    return tblName_;
  }

  /**
   * Get columns for automatic partition pruning which correspond
   * to this one
   *
   * @param analyzer Analyzer
   * @param tbl_ Table
   * @return Columns for automatic partition pruning
   * @throws AnalysisException
   */
  public LinkedList<SlotRef> getColumnsForAutomaticPartitioning(
      Analyzer analyzer, HdfsTable tbl_, Pair<Expr, Expr> between_bounds) throws AnalysisException {

    LinkedList<SlotRef> slots_part = new LinkedList<SlotRef>();

    // Check if is a partitioning column
    if (getDesc().getColumn().getPosition() < tbl_
        .getNumClusteringCols())
      throw new IllegalStateException("Can not be applied because "
          + "the column (" + getColumnName()
          + ") is already a partitioning column.");

    String subtring_part = getDesc().getColumn().getName()
        + AutoPartitionPruning.PARTITIONING_SUBSTRING;

    boolean part_by_time = false;
    for (Column column : tbl_.getColumns()) {
      String column_name = column.getName();

      if (column_name.contains(subtring_part)) {

        SlotRef part_slotRef = new SlotRef(getTableName(), column_name);
        part_slotRef.analyze(analyzer.getAnalzer(getSlotId()));
        analyzer.createIdentityEquivClasses();
        slots_part.add(part_slotRef);

        if(column_name.endsWith(AutoPartitionPruning.PARTITIONING_SUBSTRING + "year")
            || column_name.endsWith(AutoPartitionPruning.PARTITIONING_SUBSTRING + "month")
            || column_name.endsWith(AutoPartitionPruning.PARTITIONING_SUBSTRING + "day")
            || column_name.endsWith(AutoPartitionPruning.PARTITIONING_SUBSTRING + "hour")){
          part_by_time = true;
        }
      }
    }

    if (slots_part.size() < 1)
      throw new IllegalStateException("Can not be applied because "
          + "the column (" + getColumnName()
          + ") doesn't have any column for partitioning.");

    //take care with partitioning by time and several columns
    if(slots_part.size() > 1){
      if(part_by_time){

        slots_part = AutoPartitionPruning.getPartitioningColumnsForTime(slots_part, between_bounds);
      }else{
        throw new IllegalStateException("several partitioning columns are not compatible"
            + " unless these functions were year, month, day or hour.");
      }
    }

    return slots_part;
  }

  public FunctionCallExpr getPartitionFunction(Expr binding) throws AnalysisException {
    int index_part = col_.lastIndexOf(AutoPartitionPruning.PARTITIONING_SUBSTRING)
              + AutoPartitionPruning.PARTITIONING_SUBSTRING.length();
    int index_end_func = col_.indexOf('_', index_part);

    String function_name = col_.substring(
        index_part,
        index_end_func > index_part ? index_end_func : col_.length() );

    AutoPartitionPruning.checkFunctionCompatibility(function_name);

    List<Expr> params = new LinkedList<Expr>();
    params.add(binding);

    if (function_name.equals("mod")) {
      int module = Integer.valueOf(col_.substring(
          col_.lastIndexOf("_") + 1, col_.length()));

      params.add(new NumericLiteral(new BigDecimal(module)));

      function_name = "pmod";
    }else if(function_name.equals("div")){
      int divisor = Integer.valueOf(col_.substring(
          col_.lastIndexOf("_") + 1, col_.length()));

      ArithmeticExpr arit = new ArithmeticExpr(Operator.DIVIDE, binding, new NumericLiteral(new BigDecimal(divisor)));
      params.remove(binding);
      params.add(arit);

      function_name = "floor";
    }else if(function_name.equals("nummonths")){
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
    //year('2012-08-17 05:14:43') * 12 + month('2012-08-17 05:14:43')

    return new FunctionCallExpr(function_name, params);
  }

}

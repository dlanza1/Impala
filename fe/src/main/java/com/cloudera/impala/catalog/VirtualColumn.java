package com.cloudera.impala.catalog;

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
}

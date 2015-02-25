package com.cloudera.impala.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ColumnTests {

  @Test
  public void detectVirtual(){
    Column col;

    col = Column.create("t_part_year", Type.NULL, null, 0);
    assertTrue(col instanceof VirtualColumn);
    assertEquals("t", ((VirtualColumn) col).getColumnNameInWhichApplies());

    col = Column.create("name_of_the_column_part_year", Type.NULL, null, 0);
    assertTrue(col instanceof VirtualColumn);
    assertEquals("name_of_the_column", ((VirtualColumn) col).getColumnNameInWhichApplies());

    col = Column.create("_part_year", Type.NULL, null, 0);
    assertFalse(col instanceof VirtualColumn);

    col = Column.create("column_name", Type.NULL, null, 0);
    assertFalse(col instanceof VirtualColumn);

    col = Column.create("_part_", Type.NULL, null, 0);
    assertFalse(col instanceof VirtualColumn);

    col = Column.create("t_part_", Type.NULL, null, 0);
    assertFalse(col instanceof VirtualColumn);
  }

  @Test
  public void setColumnToBeApplied(){
    VirtualColumn col_virtual = (VirtualColumn) Column.create("t_part_year", Type.NULL, null, 0);
    Column col_no_virtual = Column.create("column_name", Type.NULL, null, 0);

    try {
      col_virtual.setColumnToBeApplied(col_virtual);
      assertTrue(false);
    } catch (TableLoadingException e) {
      assertTrue(true);
      e.printStackTrace();
    }

    try {
      col_virtual.setColumnToBeApplied(col_no_virtual);
      assertTrue(true);
    } catch (TableLoadingException e) {
      assertTrue(false);
      e.printStackTrace();
    }
  }

  @Test
  public void add(){
    VirtualColumn col_virtual = (VirtualColumn) Column.create("t_part_year", Type.NULL, null, 0);
    Column col_no_virtual = Column.create("column_name", Type.NULL, null, 0);

    try {
      col_virtual.addApplicableColumn(col_virtual);
      assertTrue(false);
    } catch (TableLoadingException e) {
      assertTrue(true);
      e.printStackTrace();
    }

    try {
      col_no_virtual.addApplicableColumn(col_virtual);
      assertTrue(true);
    } catch (TableLoadingException e) {
      assertTrue(false);
      e.printStackTrace();
    }
  }
}

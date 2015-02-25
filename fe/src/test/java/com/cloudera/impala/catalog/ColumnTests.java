package com.cloudera.impala.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ColumnTests {

  @Test
  public void virtualFeatures(){
    Column col;

    col = new Column("t_part_year", Type.NULL, 0);
    assertTrue(col.isVirtual());
    assertEquals("t", col.getColumnToBeApplied());

    col = new Column("name_of_the_column_part_year", Type.NULL, 0);
    assertTrue(col.isVirtual());
    assertEquals("name_of_the_column", col.getColumnToBeApplied());

    col = new Column("_part_year", Type.NULL, 0);
    assertFalse(col.isVirtual());
    assertNull(col.getColumnToBeApplied());

    col = new Column("column_name", Type.NULL, 0);
    assertFalse(col.isVirtual());
    assertNull(col.getColumnToBeApplied());

    col = new Column("_part_", Type.NULL, 0);
    assertFalse(col.isVirtual());
    assertNull(col.getColumnToBeApplied());

    col = new Column("t_part_", Type.NULL, 0);
    assertFalse(col.isVirtual());
    assertNull(col.getColumnToBeApplied());
  }

}

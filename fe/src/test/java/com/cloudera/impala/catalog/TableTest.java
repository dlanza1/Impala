package com.cloudera.impala.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.cloudera.impala.common.AnalysisException;

public class TableTest {

  @Test
  public void virtualFeatures() {
    HdfsTable tab = new HdfsTable(null, null, null, "tableName", null);

    Column col1 = Column.create("col_1", Type.NULL, null, 1);
    tab.addColumn(col1);
    Column col2 = Column.create("col_2", Type.NULL, null, 2);
    tab.addColumn(col1);
    Column col3 = Column.create("col_1", Type.NULL, null, 3);
    tab.addColumn(col1);
    Column col4 = Column.create("col_1_part_mod_500", Type.NULL, null, 4);
    tab.addColumn(col4);

    assertFalse(col1.canBeAppliedAutomaticPartitionPrunning());
    assertFalse(col2.canBeAppliedAutomaticPartitionPrunning());
    assertFalse(col3.canBeAppliedAutomaticPartitionPrunning());
    assertFalse(col4.canBeAppliedAutomaticPartitionPrunning());

    tab.computeVirtualColumns();

    assertTrue(col1.canBeAppliedAutomaticPartitionPrunning());
    assertEquals(col1, ((VirtualColumn) col4).getColumnInWhichApplies());
    try {
      assertEquals(col1.getAplicableColumns(null).getFirst(), col4);
    } catch (AnalysisException e) {
      e.printStackTrace();
    }
    assertFalse(col2.canBeAppliedAutomaticPartitionPrunning());
    assertFalse(col3.canBeAppliedAutomaticPartitionPrunning());
    assertFalse(col4.canBeAppliedAutomaticPartitionPrunning());
  }

}

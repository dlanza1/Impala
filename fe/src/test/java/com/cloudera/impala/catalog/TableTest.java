package com.cloudera.impala.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.junit.Test;

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.TTableDescriptor;

public class TableTest {

  @Test
  public void virtualFeatures(){
    Table tab = new Table(null, null, null, "table_name", null) {

      @Override
      public TTableDescriptor toThriftDescriptor(Set<Long> referencedPartitions) {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public void load(Table oldValue, HiveMetaStoreClient client,
          org.apache.hadoop.hive.metastore.api.Table msTbl) throws TableLoadingException {
        // TODO Auto-generated method stub

      }

      @Override
      public int getNumNodes() {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public TCatalogObjectType getCatalogObjectType() {
        // TODO Auto-generated method stub
        return null;
      }
    };

    Column col1 = Column.create("col_1", Type.NULL, null, 1);
    tab.addColumn(col1);
    Column col2 = Column.create("col_2", Type.NULL, null, 2);
    tab.addColumn(col1);
    Column col3 = Column.create("col_1", Type.NULL, null, 3);
    tab.addColumn(col1);
    Column col4 = Column.create("col_1_part_month", Type.NULL, null, 4);
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

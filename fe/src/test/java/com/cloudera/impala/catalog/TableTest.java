package com.cloudera.impala.catalog;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.junit.Test;

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

    tab.addColumn(new Column("col_1", Type.NULL, 1));
    tab.addColumn(new Column("col_2", Type.NULL, 2));
    tab.addColumn(new Column("col_3", Type.NULL, 3));
    assertFalse(tab.hasVirtualColumns());

    tab.addColumn(new Column("col_4_part_month", Type.NULL, 4));
    assertTrue(tab.hasVirtualColumns());

    tab.clearColumns();
    assertFalse(tab.hasVirtualColumns());

    tab.addColumn(new Column("col_1_part_month", Type.NULL, 1));
    assertTrue(tab.hasVirtualColumns());

    tab.addColumn(new Column("col_2", Type.NULL, 2));
    tab.addColumn(new Column("col_3", Type.NULL, 3));
    tab.addColumn(new Column("col_4", Type.NULL, 4));
    assertTrue(tab.hasVirtualColumns());

    tab.addColumn(new Column("col_5_part_month", Type.NULL, 5));
    assertTrue(tab.hasVirtualColumns());
  }

}

package com.cloudera.impala.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.LinkedList;

import org.junit.Test;

import com.cloudera.impala.analysis.CastExpr;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.StringLiteral;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.Pair;

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

  @Test
  public void getAplicableColumns() throws TableLoadingException, AnalysisException{
    Column column = new Column("c1", Type.NULL, 0);
    VirtualColumn v1 = new VirtualColumn("c1_part_mod_12", Type.NULL, 1);
    column.addApplicableColumn(v1);
    VirtualColumn v2 = new VirtualColumn("c1_part_mod_13", Type.NULL, 2);
    column.addApplicableColumn(v2);
    LinkedList<VirtualColumn> applicable = column.getAplicableColumns(null);
    assertEquals(applicable.size(), 1);
    assertEquals(applicable.getFirst(), v2);

    column = new Column("c1", Type.NULL, 0);
    column.addApplicableColumn(new VirtualColumn("c1_part_year", Type.NULL, 1));
    try{
      applicable = column.getAplicableColumns(null);
    }catch(AnalysisException e){
      fail();
    }

    column = new Column("c1", Type.NULL, 0);
    column.addApplicableColumn(new VirtualColumn("c1_part_month", Type.NULL, 1));
    try{
      applicable = column.getAplicableColumns(null);
      fail();
    }catch(AnalysisException e){
    }

    column = new Column("c1", Type.NULL, 0);
    column.addApplicableColumn(new VirtualColumn("c1_part_year", Type.NULL, 1));
    column.addApplicableColumn(new VirtualColumn("c1_part_month", Type.NULL, 1));
    try{
      applicable = column.getAplicableColumns(null);
      fail();
    }catch(AnalysisException e){}

    column = new Column("c1", Type.NULL, 0);
    v1 = new VirtualColumn("c1_part_year", Type.NULL, 1);
    v2 = new VirtualColumn("c1_part_month", Type.NULL, 1);
    VirtualColumn v3 = new VirtualColumn("c1_part_day", Type.NULL, 1);
    column.addApplicableColumn(v1);
    column.addApplicableColumn(v2);
    column.addApplicableColumn(v3);
    try{
      Pair<Expr, Expr> be= new Pair<Expr, Expr>(
          new CastExpr(Type.TIMESTAMP, new StringLiteral("1995-10-25 17:45:30.005"), false),
          new CastExpr(Type.TIMESTAMP, new StringLiteral("1995-10-30 17:45:30.005"), false));
      applicable = column.getAplicableColumns(be);

      assertEquals(applicable.size(), 3);
      assertEquals(applicable.get(0), v1);
      assertEquals(applicable.get(1), v2);
      assertEquals(applicable.get(2), v3);
    }catch(AnalysisException e){
      fail();
      e.printStackTrace();
    }

    column = new Column("c1", Type.NULL, 0);
    column.addApplicableColumn(new VirtualColumn("c1_part_month", Type.NULL, 1));
    try{
      Pair<Expr, Expr> be= new Pair<Expr, Expr>(
          new CastExpr(Type.TIMESTAMP, new StringLiteral("1995-10-25 17:45:30.005"), false),
          new CastExpr(Type.TIMESTAMP, new StringLiteral("1995-10-30 17:45:30.005"), false));
      applicable = column.getAplicableColumns(be);

      fail();
    }catch(AnalysisException e){}

    column = new Column("c1", Type.NULL, 0);
    column.addApplicableColumn(new VirtualColumn("c1_part_month", Type.NULL, 1));
    try{
      applicable = column.getAplicableColumns(null);

      fail();
    }catch(AnalysisException e){}

    column = new Column("c1", Type.NULL, 0);
    column.addApplicableColumn(new VirtualColumn("c1_part_mod_7", Type.NULL, 1));
    try{
      Pair<Expr, Expr> be= new Pair<Expr, Expr>(
          new CastExpr(Type.TIMESTAMP, new StringLiteral("1995-10-25 17:45:30.005"), false),
          new CastExpr(Type.TIMESTAMP, new StringLiteral("1995-10-30 17:45:30.005"), false));
      applicable = column.getAplicableColumns(be);

      fail();
    }catch(AnalysisException e){}

    column = new Column("c1", Type.NULL, 0);
    v1 = new VirtualColumn("c1_part_mod_7", Type.NULL, 1);
    column.addApplicableColumn(v1);
    column.addApplicableColumn(new VirtualColumn("c1_part_year", Type.NULL, 1));
    column.addApplicableColumn(new VirtualColumn("c1_part_month", Type.NULL, 1));
    try{
      applicable = column.getAplicableColumns(null);

      assertEquals(applicable.size(), 1);
      assertEquals(applicable.getFirst(), v1);
    }catch(AnalysisException e){
      fail();
    }

  }
}

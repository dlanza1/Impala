package com.cloudera.impala.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.math.BigDecimal;

import org.junit.Test;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.FunctionCallExpr;
import com.cloudera.impala.analysis.NumericLiteral;
import com.cloudera.impala.common.AnalysisException;

public class VirtualColumnTests {

  @Test
  public void checkContainSubstring(){
    try {
      new VirtualColumn("name_day", Type.NULL, 0);
      fail();
    } catch (AnalysisException e) {
      e.printStackTrace();
    }

    try {
      new VirtualColumn("name_part_", Type.NULL, 0);
      fail();
    } catch (AnalysisException e) {
      e.printStackTrace();
    }

    try {
      new VirtualColumn("name_part_day", Type.NULL, 0);
    } catch (AnalysisException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void gettingFunction(){
    try {
      VirtualColumn vc = new VirtualColumn("name_part_day", Type.NULL, 0);

      assertEquals(vc.function, VirtualColumn.Function.DAY);
    } catch (AnalysisException e) {
      e.printStackTrace();
      fail();
    }

    try {
      VirtualColumn vc = new VirtualColumn("name_part_nummonths", Type.NULL, 0);

      assertEquals(vc.function, VirtualColumn.Function.NUMMONTHS);

      NumericLiteral tmp = new NumericLiteral(new BigDecimal(3));

      assertEquals(((FunctionCallExpr) vc.function.get(tmp).getChild(0).getChild(0).getChild(0))
          .getFnName().getFunction(), "year");
      assertEquals(vc.function.get(tmp).getChild(0).getChild(0).getChild(0).getChild(0), tmp);
      assertEquals(((FunctionCallExpr) vc.function.get(tmp).getChild(0).getChild(1))
          .getFnName().getFunction(), "month");
      assertEquals(vc.function.get(tmp).getChild(0).getChild(1).getChild(0), tmp);
    } catch (AnalysisException e) {
      e.printStackTrace();
      fail();
    }

    try {
      VirtualColumn vc = new VirtualColumn("name_part_numdays", Type.NULL, 0);

      assertEquals(vc.function, VirtualColumn.Function.NUMDAYS);

      NumericLiteral tmp = new NumericLiteral(new BigDecimal(3));

      assertEquals(((FunctionCallExpr) vc.function.get(tmp).getChild(0).getChild(0).getChild(0))
          .getFnName().getFunction(), "year");
      assertEquals(vc.function.get(tmp).getChild(0).getChild(0).getChild(0).getChild(0), tmp);
      assertEquals(((FunctionCallExpr) vc.function.get(tmp).getChild(0).getChild(1))
          .getFnName().getFunction(), "dayofyear");
      assertEquals(vc.function.get(tmp).getChild(0).getChild(1).getChild(0), tmp);
    } catch (AnalysisException e) {
      e.printStackTrace();
      fail();
    }

    try {
      new VirtualColumn("name_part_abcd", Type.NULL, 0);
      fail();
    } catch (AnalysisException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void args(){

    try {
      new VirtualColumn("name_part_day_", Type.NULL, 0);
      fail();
    } catch (AnalysisException e) {
      e.printStackTrace();
    }

    try {
      VirtualColumn vc = new VirtualColumn("name_part_day", Type.NULL, 0);

      assertNull(vc.function.args);
      Expr tmp = new NumericLiteral(new BigDecimal(10));
      assertEquals(tmp, vc.function.get(tmp).getChild(0));
    } catch (AnalysisException e) {
      e.printStackTrace();
      fail();
    }

    try {
      VirtualColumn vc = new VirtualColumn("name_part_mod_500", Type.NULL, 0);

      NumericLiteral tmp = new NumericLiteral(new BigDecimal(929658));
      assertEquals(vc.function.get(tmp).getChild(0), tmp);
      assertEquals(vc.function.get(tmp).getChild(1), new NumericLiteral(new BigDecimal(500)));
      assertEquals(vc.function.args.length, 1);
    } catch (AnalysisException e) {
      e.printStackTrace();
      fail();
    }

    try {
      VirtualColumn vc = new VirtualColumn("name_part_div_22", Type.NULL, 0);

      NumericLiteral tmp = new NumericLiteral(new BigDecimal(3));
      assertEquals(vc.function.get(tmp).getChild(0).getChild(0), tmp);
      assertEquals(vc.function.get(tmp).getChild(0).getChild(1),
                                    new NumericLiteral(new BigDecimal(22)));
      assertEquals(vc.function.args.length, 1);
    } catch (AnalysisException e) {
      e.printStackTrace();
      fail();
    }

    try {
      new VirtualColumn("name_part_mod_mod", Type.NULL, 0);
      fail();
    } catch (AnalysisException e) {
      e.printStackTrace();
    }

    try {
      new VirtualColumn("name_part_div_2d2", Type.NULL, 0);
      fail();
    } catch (AnalysisException e) {
      e.printStackTrace();
    }

  }


}

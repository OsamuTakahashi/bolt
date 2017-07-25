package com.sopranoworks.bolt

import com.google.cloud.spanner._
import org.specs2.mutable.Specification

import scala.collection.JavaConversions._

class TableColumnValueTest extends Specification {
  class DummyDatabase extends Database {
    var tables = Map.empty[String,Table]
    override def table(name: String): Option[Table] = tables.get(name)
  }

  class DummyNat extends Bolt.Nat(null) {
    private val _database = new DummyDatabase
    override def database: Database = _database

    private var _queryCount = 0
    def queryCount = _queryCount

    override def executeNativeQuery(sql: String): ResultSet = {
      val sb = Struct.newBuilder()
      sb.add("ONE",Value.int64(1))
      sb.add("TWO",Value.int64(2))
      sb.add("THREE",Value.int64(2))

      _queryCount += 1

      ResultSets.forRows(Type.struct(List(Type.StructField.of("ONE",Type.int64()),Type.StructField.of("TWO",Type.int64()),Type.StructField.of("THREE",Type.int64()) )),List(sb.build()))
    }
  }

  "resolveReference" should {
    "gathering" in {
      val col1 = TableColumnValue("ONE","TEST_TABLE",0)
      val col2 = TableColumnValue("TWO","TEST_TABLE",1)
      val col3 = TableColumnValue("THREE","TEST_TABLE",2)

      val ex = ExpressionValue("+",ExpressionValue("+",ExpressionValue("+",col1,col2),col3),IntValue("1",1,true))

      val cols = ex.resolveReference()
      cols.values.size must_== 3
    }
    "using at multiple positions" in {
      val col1 = TableColumnValue("ONE","TEST_TABLE",0)
      val col2 = TableColumnValue("TWO","TEST_TABLE",1)

      val ex = ExpressionValue("+",ExpressionValue("+",ExpressionValue("+",col1,col2),col2),IntValue("1",1,true))

      val cols = ex.resolveReference()
      cols.values.size must_== 2
    }
  }
  "eavluation after setting each values" should {
    "normal exression" in {
      val col1 = TableColumnValue("ONE","TEST_TABLE",0)
      val col2 = TableColumnValue("TWO","TEST_TABLE",1)
      val col3 = TableColumnValue("THREE","TEST_TABLE",2)

      val ex = ExpressionValue("+",ExpressionValue("+",ExpressionValue("+",col1,col2),col3),IntValue("1",1,true))

      val cols = ex.resolveReference()
      cols.values.size must_== 3

      col1.setValue(IntValue("1",1,true))
      col2.setValue(IntValue("2",2,true))
      col3.setValue(IntValue("3",3,true))
      ex.eval.asValue.asInstanceOf[IntValue].value must_== 7
    }
  }
  "invalidateEvaluatedValueIfContains" should {
    "TableColumnValue" in {
      val col1 = TableColumnValue("ONE","TEST_TABLE",0)
      col1.resolveReference()
      col1.setValue(IntValue("1",1,true))
      col1.eval.asValue.asInstanceOf[IntValue].value must_== 1
      col1.invalidateEvaluatedValueIfContains(List(col1)) must_== true
      col1.resolveReference()
      col1.setValue(IntValue("10",10,true))
      col1.eval.asValue.asInstanceOf[IntValue].value must_== 10
    }
    "simple expression" in {
      val col1 = TableColumnValue("ONE","TEST_TABLE",0)
      val col2 = TableColumnValue("TWO","TEST_TABLE",1)
      val ex = ExpressionValue("+",col1,col2)

      val cols = ex.resolveReference()
      cols.values.size must_== 2

      col1.setValue(IntValue(1))
      col2.setValue(IntValue(2))
      ex.eval.asValue.asInstanceOf[IntValue].value must_== 3

      ex.invalidateEvaluatedValueIfContains(List(col1,col2)) must_== true
      ex.resolveReference()
      col1.setValue(IntValue(10))
      col2.setValue(IntValue(20))
      ex.eval.asValue.asInstanceOf[IntValue].value must_== 30
    }
    "invalidate after evaluation" in {
      val col1 = TableColumnValue("ONE","TEST_TABLE",0)
      val col2 = TableColumnValue("TWO","TEST_TABLE",1)
      val col3 = TableColumnValue("THREE","TEST_TABLE",2)

      val ex = ExpressionValue("+",ExpressionValue("+",ExpressionValue("+",col1,col2),col3),IntValue("1",1,true))

      val cols = ex.resolveReference()
      cols.values.size must_== 3

      col1.setValue(IntValue("1",1,true))
      col2.setValue(IntValue("2",2,true))
      col3.setValue(IntValue("3",3,true))
      ex.eval.asValue.asInstanceOf[IntValue].value must_== 7

      ex.invalidateEvaluatedValueIfContains(List(col1,col2,col3)) must_== true
      ex.resolveReference()
      col1.setValue(IntValue("10",10,true))
      col2.setValue(IntValue("20",20,true))
      col3.setValue(IntValue("30",30,true))
      ex.eval.asValue.asInstanceOf[IntValue].value must_== 61
    }
    "invalidate after evaluation containing subquery" in {
      val nat = new DummyNat
      val qc = QueryContext(nat,null)

      val col1 = TableColumnValue("ONE","TEST_TABLE",0)
      val col2 = TableColumnValue("TWO","TEST_TABLE",1)
      val col3 = TableColumnValue("THREE","TEST_TABLE",2)

      val ex = ExpressionValue("+",ExpressionValue("+",ExpressionValue("+",col1,col2),col3),ResultIndexValue(SubqueryValue(nat,"SELECT *",qc),0))

      val cols = ex.resolveReference()
      cols.values.size must_== 3

      col1.setValue(IntValue("1",1,true))
      col2.setValue(IntValue("2",2,true))
      col3.setValue(IntValue("3",3,true))
      ex.eval.asValue.asInstanceOf[IntValue].value must_== 7

      ex.invalidateEvaluatedValueIfContains(List(col1,col2,col3)) must_== true
      ex.resolveReference()
      col1.setValue(IntValue("10",10,true))
      col2.setValue(IntValue("20",20,true))
      col3.setValue(IntValue("30",30,true))
      ex.eval.asValue.asInstanceOf[IntValue].value must_== 61
      nat.queryCount must_== 1
    }
    "invalidate not all of tablecolumnvalue" in {
      val col1 = TableColumnValue("ONE","TEST_TABLE",0)
      val col2 = TableColumnValue("TWO","TEST_TABLE",1)
      val col3 = TableColumnValue("THREE","TEST_TABLE",2)

      val ex = ExpressionValue("+",ExpressionValue("+",ExpressionValue("+",col1,col2),col3),IntValue("1",1,true))

      val cols = ex.resolveReference()
      cols.values.size must_== 3

      col1.setValue(IntValue("1",1,true))
      col2.setValue(IntValue("2",2,true))
      col3.setValue(IntValue("3",3,true))
      ex.eval.asValue.asInstanceOf[IntValue].value must_== 7

      ex.invalidateEvaluatedValueIfContains(List(col1,col2)) must_== true
      ex.resolveReference()
      col1.setValue(IntValue("10",10,true))
      col2.setValue(IntValue("20",20,true))
      ex.eval.asValue.asInstanceOf[IntValue].value must_== 34
    }
    /*"invalidate after evaluation using alias" in {
      val nat = new DummyNat
      val qc = QueryContext(nat,null)
      qc.addAlias()

      val col1 = TableColumnValue("ONE","TEST_TABLE",0)
      val col2 = TableColumnValue("TWO","TEST_TABLE",1)
      val col3 = TableColumnValue("THREE","TEST_TABLE",2)

      val ex = ExpressionValue("+",ExpressionValue("+",ExpressionValue("+",col1,col2),col3),IntValue("1",1,true))

      val cols = ex.resolveReference()
      cols.values.size must_== 3

      col1.setValue(IntValue("1",1,true))
      col2.setValue(IntValue("2",2,true))
      col3.setValue(IntValue("3",3,true))
      ex.eval.asValue.asInstanceOf[IntValue].value must_== 7

      ex.invalidateEvaluatedValueIfContains(List(col1,col2,col3)) must_== true
      ex.resolveReference()
      col1.setValue(IntValue("10",10,true))
      col2.setValue(IntValue("20",20,true))
      col3.setValue(IntValue("30",30,true))
      ex.eval.asValue.asInstanceOf[IntValue].value must_== 61
    } */
  }
}

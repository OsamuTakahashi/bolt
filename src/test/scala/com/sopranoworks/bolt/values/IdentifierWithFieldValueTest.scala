package com.sopranoworks.bolt.values

import com.sopranoworks.bolt._

import com.google.cloud.spanner.{ResultSet, ResultSets, Struct, Type, Value=>SValue, Database => SDatabase}
import org.specs2.mutable.Specification

import scala.collection.JavaConversions._

class IdentifierWithFieldValueTest extends Specification {
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
      sb.add("ONE",SValue.int64(1))
      sb.add("TWO",SValue.int64(2))
      sb.add("THREE",SValue.int64(2))

      _queryCount += 1

      ResultSets.forRows(Type.struct(List(Type.StructField.of("ONE",Type.int64()),Type.StructField.of("TWO",Type.int64()),Type.StructField.of("THREE",Type.int64()) )),List(sb.build()))
    }
  }

  "resolveReference" should {
    "column name" in {
      val tbl = Table(null,"TEST_TABLE",
        List(Column("x",0,"INT64",false),Column("y",1,"INT64",false),Column("y",2,"INT64",false)),
        Index("PRIMARY_KEY",List(IndexColumn("x",0,"INT64",false,"ASC"))),
        Map.empty[String,Index])
      val nat = new DummyNat
      nat.database.asInstanceOf[DummyDatabase].tables += ("TEST_TABLE"->tbl)
      val qc = QueryContext(nat,null)

      val v = IdentifierWithFieldValue("TEST_TABLE",List("x"),qc)
      v.resolveReference()
      v.eval.apply().isInstanceOf[TableColumnValue] must_== true
      v().asInstanceOf[TableColumnValue].text must_== "TEST_TABLE.x"
    }
    "subquery field name" in {
      val nat = new DummyNat
      val qc = QueryContext(nat,null)
      qc.setSubquery(SubqueryValue(nat,"SELECT *",qc))
      qc.addAlias(ExpressionAlias("x",qc.subquery.get))

      val v = IdentifierWithFieldValue("x",List("ONE"),qc)
      v.resolveReference()
      v.eval.asValue.isInstanceOf[IntValue] must_== true
      v.asValue.asInstanceOf[IntValue].value must_== 1
    }
    "struct field name" in {
      val st = StructValue()
      st.addValue(IntValue("ONE",1,true))
      st.addValue(IntValue("TWO",2,true))
      st.addValue(IntValue("THREE",3,true))
      st.addFieldName("ONE",0)
      st.addFieldName("TWO",1)
      st.addFieldName("THREE",2)

      val nat = new DummyNat
      val qc = QueryContext(nat,null)
      qc.addAlias(ExpressionAlias("x",st))

      val v = IdentifierWithFieldValue("x",List("ONE"),qc)
      v.resolveReference()
      v.eval.asValue.isInstanceOf[IntValue] must_== true
      v.asValue.asInstanceOf[IntValue].value must_== 1
    }
    "alias table column name" in {
      val tbl = Table(null,"TEST_TABLE",
        List(Column("x",0,"INT64",false),Column("y",1,"INT64",false),Column("y",2,"INT64",false)),
        Index("PRIMARY_KEY",List(IndexColumn("x",0,"INT64",false,"ASC"))),
        Map.empty[String,Index])
      val nat = new DummyNat
      nat.database.asInstanceOf[DummyDatabase].tables += ("TEST_TABLE"->tbl)
      val qc = QueryContext(nat,null)
      qc.addAlias(new TableAlias("T","TEST_TABLE"))
      val v = IdentifierWithFieldValue("T",List("x"),qc)

      v.resolveReference()
      v.eval.apply().isInstanceOf[TableColumnValue] must_== true
      v().asInstanceOf[TableColumnValue].text must_== "TEST_TABLE.x"
    }
  }
}

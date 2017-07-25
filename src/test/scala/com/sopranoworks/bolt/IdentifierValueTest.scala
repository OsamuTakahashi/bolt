package com.sopranoworks.bolt

import com.google.cloud.spanner._
import org.specs2.mutable.Specification

import scala.collection.JavaConversions._

class IdentifierValueTest extends Specification {

  class DummyDatabase extends Database {
    var tables = Map.empty[String,Table]
    override def table(name: String): Option[Table] = tables.get(name)
  }

  class DummyNat extends Bolt.Nat(null) {
    private val _database = new DummyDatabase
    override def database: Database = _database

    override def executeNativeQuery(sql: String): ResultSet = {
      val sb = Struct.newBuilder()
      sb.add("ONE",Value.int64(1))
      sb.add("TWO",Value.int64(2))
      sb.add("THREE",Value.int64(2))

      ResultSets.forRows(Type.struct(List(Type.StructField.of("ONE",Type.int64()),Type.StructField.of("TWO",Type.int64()),Type.StructField.of("THREE",Type.int64()) )),List(sb.build()))
    }
  }

  "resolveReference" should {
    "is TableAlias" in {
      val nat = new DummyNat
      val qc = QueryContext(nat,null)
      qc.addAlias(TableAlias("T","TEST_TABLE"))

      val v = IdentifierValue("T",qc)
      v.resolveReference()
      v.asValue.isInstanceOf[TableValue] must_== true
      v.asValue.asInstanceOf[TableValue].name must_== "TEST_TABLE"
    }
    "is QueryResultAlias " in {
      val nat = new DummyNat
      val qc = QueryContext(nat,null)
      qc.setSubquery(SubqueryValue(nat,"SELECT *",qc))
      qc.addAlias(QueryResultAlias("x",0,qc))

      val v = IdentifierValue("x",qc)
      v.resolveReference()

      v.asValue.isInstanceOf[IntValue] must_== true
      v.asValue.asInstanceOf[IntValue].value must_== 1
    }
    "is QueryResultAlias without subquery" in {
      val nat = new DummyNat
      val qc = QueryContext(nat,null)
      qc.addAlias(QueryResultAlias("x",0,qc))

      val v = IdentifierValue("x",qc)
      v.resolveReference()

      v.asValue must_== NullValue
    }
    "is ExpressionAlias" in {
      val nat = new DummyNat
      val qc = QueryContext(nat,null)
      qc.addAlias(ExpressionAlias("x",ExpressionValue("+",IntValue("1",1,true),IntValue("2",2,true))))

      val v = IdentifierValue("x",qc)
      v.resolveReference()

      v.asValue.isInstanceOf[IntValue] must_== true
      v.asValue.asInstanceOf[IntValue].value must_== 3
    }
    "is table column name" in {
      val tbl = Table(null,"TEST_TABLE",
        List(Column("x",0,"INT64",false),Column("y",1,"INT64",false),Column("y",2,"INT64",false)),
        Index("PRIMARY_KEY",List(IndexColumn("x",0,"INT64",false,"ASC"))),
        Map.empty[String,Index])
      val nat = new DummyNat
      nat.database.asInstanceOf[DummyDatabase].tables += ("TEST_TABLE"->tbl)
      val qc = QueryContext(nat,null)
      qc.setCurrentTable("TEST_TABLE")

      val v = IdentifierValue("x",qc)
      v.resolveReference()
      v.asValue.isInstanceOf[TableColumnValue] must_== true
      v.asValue.asInstanceOf[TableColumnValue].text must_== "TEST_TABLE.x"
    }
    "is table name" in {
      val tbl = Table(null,"TEST_TABLE",
        List(Column("x",0,"INT64",false),Column("y",1,"INT64",false),Column("y",2,"INT64",false)),
        Index("PRIMARY_KEY",List(IndexColumn("x",0,"INT64",false,"ASC"))),
        Map.empty[String,Index])
      val nat = new DummyNat
      nat.database.asInstanceOf[DummyDatabase].tables += ("TEST_TABLE"->tbl)
      val qc = QueryContext(nat,null)
      
      val v = IdentifierValue("TEST_TABLE",qc)
      v.resolveReference()
      v.asValue.isInstanceOf[TableValue] must_== true
      v.asValue.asInstanceOf[TableValue].name must_== "TEST_TABLE"
    }
  }
}

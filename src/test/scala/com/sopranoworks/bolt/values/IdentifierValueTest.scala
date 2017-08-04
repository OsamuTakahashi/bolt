package com.sopranoworks.bolt.values

import com.sopranoworks.bolt._

import com.google.cloud.spanner.{ResultSet, ResultSets, Struct, Type, Value=>SValue, Database => SDatabase}
import org.specs2.mutable.Specification

import scala.collection.JavaConversions._

class IdentifierValueTest extends Specification {

  class DummyDatabase extends Database {
    var tables = Map.empty[String,Table]
    override def table(name: String): Option[Table] = tables.get(name)
  }

  class DummyNut extends Bolt.Nut(null) {
    private val _database = new DummyDatabase
    override def database: Database = _database

    override def executeNativeQuery(sql: String): ResultSet = {
      val sb = Struct.newBuilder()
      sb.add("ONE",SValue.int64(1))
      sb.add("TWO",SValue.int64(2))
      sb.add("THREE",SValue.int64(2))

      ResultSets.forRows(Type.struct(List(Type.StructField.of("ONE",Type.int64()),Type.StructField.of("TWO",Type.int64()),Type.StructField.of("THREE",Type.int64()) )),List(sb.build()))
    }
  }

  "resolveReference" should {
    "is TableAlias" in {
      val nat = new DummyNut
      val qc = QueryContext(nat,null)
      qc.addAlias(TableAlias("T","TEST_TABLE"))

      val v = IdentifierValue("T",qc)
      v.resolveReference()
      v.asValue.isInstanceOf[TableValue] must_== true
      v.asValue.asInstanceOf[TableValue].name must_== "TEST_TABLE"
    }
    "is QueryResultAlias " in {
      val nat = new DummyNut
      val qc = QueryContext(nat,null)
      qc.setSubquery(SubqueryValue(nat,"SELECT *",qc))
      qc.addAlias(QueryResultAlias("x",0,qc))

      val v = IdentifierValue("x",qc)
      v.resolveReference()

      v.asValue.isInstanceOf[IntValue] must_== true
      v.asValue.asInstanceOf[IntValue].value must_== 1
    }
    "is QueryResultAlias without subquery" in {
      val nat = new DummyNut
      val qc = QueryContext(nat,null)
      qc.addAlias(QueryResultAlias("x",0,qc))

      val v = IdentifierValue("x",qc)
      v.resolveReference()

      v.asValue must_== NullValue
    }
    "is ExpressionAlias" in {
      val nat = new DummyNut
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
      val nat = new DummyNut
      nat.database.asInstanceOf[DummyDatabase].tables += ("TEST_TABLE"->tbl)
      val qc = QueryContext(nat,null)
      qc.setCurrentTable("TEST_TABLE")

      val v = IdentifierValue("x",qc)
      v.resolveReference()
      v().isInstanceOf[TableColumnValue] must_== true
      v().asInstanceOf[TableColumnValue].text must_== "TEST_TABLE.x"
    }
    "are table column names" in {
      val tbl = Table(null,"TEST_TABLE",
        List(Column("x",0,"INT64",false),Column("y",1,"INT64",false),Column("y",2,"INT64",false)),
        Index("PRIMARY_KEY",List(IndexColumn("x",0,"INT64",false,"ASC"))),
        Map.empty[String,Index])
      val nat = new DummyNut
      nat.database.asInstanceOf[DummyDatabase].tables += ("TEST_TABLE"->tbl)
      val qc = QueryContext(nat,null)
      qc.setCurrentTable("TEST_TABLE")

      val v1 = IdentifierValue("x",qc)
      val v2 = IdentifierValue("x",qc)
      val r1 = v1.resolveReference()
      val r2 = v2.resolveReference(r1)

      r2.size must_== 1

      r2.values.head.setValue(IntValue(1))

      v1.asValue.isInstanceOf[IntValue] must_== true
      v1.asValue.asInstanceOf[IntValue].value must_== 1
      v2.asValue.isInstanceOf[IntValue] must_== true
      v2.asValue.asInstanceOf[IntValue].value must_== 1
    }
    "is table name" in {
      val tbl = Table(null,"TEST_TABLE",
        List(Column("x",0,"INT64",false),Column("y",1,"INT64",false),Column("y",2,"INT64",false)),
        Index("PRIMARY_KEY",List(IndexColumn("x",0,"INT64",false,"ASC"))),
        Map.empty[String,Index])
      val nat = new DummyNut
      nat.database.asInstanceOf[DummyDatabase].tables += ("TEST_TABLE"->tbl)
      val qc = QueryContext(nat,null)
      
      val v = IdentifierValue("TEST_TABLE",qc)
      v.resolveReference()
      v.asValue.isInstanceOf[TableValue] must_== true
      v.asValue.asInstanceOf[TableValue].name must_== "TEST_TABLE"
    }
  }
}

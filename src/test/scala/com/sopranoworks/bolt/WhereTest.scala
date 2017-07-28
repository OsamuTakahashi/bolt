package com.sopranoworks.bolt

import java.io.ByteArrayInputStream

import com.google.cloud.spanner.{ResultSet, ResultSets, Struct, Type, Value => SValue}
import com.sopranoworks.bolt.values._
import org.antlr.v4.runtime.{ANTLRInputStream, BailErrorStrategy, CommonTokenStream}
import org.specs2.mutable.Specification

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

class WhereTest extends Specification {
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

  "eval" should {
    /*"no and" in {
      val nat = new DummyNat
      nat.database.asInstanceOf[DummyDatabase].tables += ("TEST_TABLE"->
        Table(null,"TEST_TABLE",List(),Index("PRIMARY_KEY",List(IndexColumn("ID",0,"INT64",false,"ASC"))),Map.empty[String,Index]))
      val qc = QueryContext(nat,null)
      qc.setCurrentTable("TEST_TABLE")
      val w = Where(qc,"TEST_TABLE","WHERE ID=0",BooleanExpressionValue("=",TableColumnValue("ID","TEST_TABLE",0),IntValue(0)))
      w.eval()
      w.isOptimizedWhere must_== true
    }
    "one and" in {
      val nat = new DummyNat
      nat.database.asInstanceOf[DummyDatabase].tables += ("TEST_TABLE"->
        Table(null,"TEST_TABLE",List(),Index("PRIMARY_KEY",List(IndexColumn("ID1",0,"INT64",false,"ASC"),IndexColumn("ID2",1,"INT64",false,"ASC"))),Map.empty[String,Index]))
      val qc = QueryContext(nat,null)
      qc.setCurrentTable("TEST_TABLE")
      val w = Where(qc,"TEST_TABLE","WHERE ID=1 AND ID2=0",BooleanExpressionValue("AND",BooleanExpressionValue("=",TableColumnValue("ID1","TEST_TABLE",0),IntValue(0)),BooleanExpressionValue("=",TableColumnValue("ID2","TEST_TABLE",0),IntValue(0))))
      w.eval()
      w.isOptimizedWhere must_== true
    } */
    "lack primary key" in {
      val nat = new DummyNat
      nat.database.asInstanceOf[DummyDatabase].tables += ("TEST_TABLE"->
        Table(null,"TEST_TABLE",List(Column("ID1",0,"INT64",false),Column("ID2",1,"INT64",false)),Index("PRIMARY_KEY",List(IndexColumn("ID1",0,"INT64",false,"ASC"),IndexColumn("ID2",1,"INT64",false,"ASC"))),Map.empty[String,Index]))
      val qc = QueryContext(nat,null)
      qc.setCurrentTable("TEST_TABLE")

      val sql = "WHERE ID1=0"
      val source = new ByteArrayInputStream(sql.getBytes("UTF8"))
      val input = new ANTLRInputStream(source)
      val lexer = new MiniSqlLexer(input)

      val tokenStream = new CommonTokenStream(lexer)
      val parser = new MiniSqlParser(tokenStream)
      parser.setErrorHandler(new BailErrorStrategy())
      parser.nat = nat
      parser.qc = qc

      val r = parser.where_stmt()
      r.where.eval()
      r.where.isOptimizedWhere must_== false
    }
    "one AND with parser" in {
      val nat = new DummyNat
      nat.database.asInstanceOf[DummyDatabase].tables += ("TEST_TABLE"->
        Table(null,"TEST_TABLE",List(Column("ID1",0,"INT64",false),Column("ID2",1,"INT64",false)),Index("PRIMARY_KEY",List(IndexColumn("ID1",0,"INT64",false,"ASC"),IndexColumn("ID2",1,"INT64",false,"ASC"))),Map.empty[String,Index]))
      val qc = QueryContext(nat,null)
      qc.setCurrentTable("TEST_TABLE")

      val sql = "WHERE ID1=0 AND ID2=1"
      val source = new ByteArrayInputStream(sql.getBytes("UTF8"))
      val input = new ANTLRInputStream(source)
      val lexer = new MiniSqlLexer(input)

      val tokenStream = new CommonTokenStream(lexer)
      val parser = new MiniSqlParser(tokenStream)
      parser.setErrorHandler(new BailErrorStrategy())
      parser.nat = nat
      parser.qc = qc

      val r = parser.where_stmt()
      r.where.eval()
      r.where.isOptimizedWhere must_== true
    }
    "one AND with parser 2" in {
      val nat = new DummyNat
      nat.database.asInstanceOf[DummyDatabase].tables += ("TEST_TABLE"->
        Table(null,"TEST_TABLE",List(Column("ID1",0,"INT64",false),Column("ID2",1,"INT64",false)),Index("PRIMARY_KEY",List(IndexColumn("ID1",0,"INT64",false,"ASC"),IndexColumn("ID2",1,"INT64",false,"ASC"))),Map.empty[String,Index]))
      val qc = QueryContext(nat,null)
      qc.setCurrentTable("TEST_TABLE")

      val sql = "WHERE ID1=0 AND ID2=1+2"
      val source = new ByteArrayInputStream(sql.getBytes("UTF8"))
      val input = new ANTLRInputStream(source)
      val lexer = new MiniSqlLexer(input)

      val tokenStream = new CommonTokenStream(lexer)
      val parser = new MiniSqlParser(tokenStream)
      parser.setErrorHandler(new BailErrorStrategy())
      parser.nat = nat
      parser.qc = qc

      val r = parser.where_stmt()
      r.where.eval()
      r.where.isOptimizedWhere must_== true
    }
    "one AND including comprer operator with parser" in {
      val nat = new DummyNat
      nat.database.asInstanceOf[DummyDatabase].tables += ("TEST_TABLE"->
        Table(null,"TEST_TABLE",List(Column("ID1",0,"INT64",false),Column("ID2",1,"INT64",false)),Index("PRIMARY_KEY",List(IndexColumn("ID1",0,"INT64",false,"ASC"),IndexColumn("ID2",1,"INT64",false,"ASC"))),Map.empty[String,Index]))
      val qc = QueryContext(nat,null)
      qc.setCurrentTable("TEST_TABLE")

      val sql = "WHERE ID1=0 AND ID2>1"
      val source = new ByteArrayInputStream(sql.getBytes("UTF8"))
      val input = new ANTLRInputStream(source)
      val lexer = new MiniSqlLexer(input)

      val tokenStream = new CommonTokenStream(lexer)
      val parser = new MiniSqlParser(tokenStream)
      parser.setErrorHandler(new BailErrorStrategy())
      parser.nat = nat
      parser.qc = qc

      val r = parser.where_stmt()
      r.where.eval()
      r.where.isOptimizedWhere must_== false
    }
    "one AND including none key column with parser" in {
      val nat = new DummyNat
      nat.database.asInstanceOf[DummyDatabase].tables += ("TEST_TABLE"->
        Table(null,"TEST_TABLE",List(Column("ID1",0,"INT64",false),Column("ID2",1,"INT64",false),Column("COL",2,"INT64",false)),Index("PRIMARY_KEY",List(IndexColumn("ID1",0,"INT64",false,"ASC"),IndexColumn("ID2",1,"INT64",false,"ASC"))),Map.empty[String,Index]))
      val qc = QueryContext(nat,null)
      qc.setCurrentTable("TEST_TABLE")

      val sql = "WHERE ID1=0 AND COL=1"
      val source = new ByteArrayInputStream(sql.getBytes("UTF8"))
      val input = new ANTLRInputStream(source)
      val lexer = new MiniSqlLexer(input)

      val tokenStream = new CommonTokenStream(lexer)
      val parser = new MiniSqlParser(tokenStream)
      parser.setErrorHandler(new BailErrorStrategy())
      parser.nat = nat
      parser.qc = qc

      val r = parser.where_stmt()
      r.where.eval()
      r.where.isOptimizedWhere must_== false
    }
    "2 AND with parser" in {
      val nat = new DummyNat
      nat.database.asInstanceOf[DummyDatabase].tables += ("TEST_TABLE"->
        Table(null,"TEST_TABLE",List(Column("ID1",0,"INT64",false),Column("ID2",1,"INT64",false),Column("ID3",2,"INT64",false)),Index("PRIMARY_KEY",List(IndexColumn("ID1",0,"INT64",false,"ASC"),IndexColumn("ID2",1,"INT64",false,"ASC"),IndexColumn("ID3",2,"INT64",false,"ASC"))),Map.empty[String,Index]))
      val qc = QueryContext(nat,null)
      qc.setCurrentTable("TEST_TABLE")

      val sql = "WHERE ID1=0 AND ID2=1 AND ID3=2"
      val source = new ByteArrayInputStream(sql.getBytes("UTF8"))
      val input = new ANTLRInputStream(source)
      val lexer = new MiniSqlLexer(input)

      val tokenStream = new CommonTokenStream(lexer)
      val parser = new MiniSqlParser(tokenStream)
      parser.setErrorHandler(new BailErrorStrategy())
      parser.nat = nat
      parser.qc = qc

      val r = parser.where_stmt()
      r.where.eval()
      r.where.isOptimizedWhere must_== true
    }
  }
}

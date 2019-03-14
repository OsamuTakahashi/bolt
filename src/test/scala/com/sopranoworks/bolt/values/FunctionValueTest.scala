package com.sopranoworks.bolt.values

import java.io.ByteArrayInputStream
import java.util

import com.google.cloud.spanner.{ResultSet, ResultSets, Struct, Type, Value => SValue}
import com.sopranoworks.bolt._
import org.antlr.v4.runtime.{ANTLRInputStream, BailErrorStrategy, CommonTokenStream}
import org.joda.time.DateTime
import org.specs2.mutable.Specification

import scala.collection.JavaConversions._

class FunctionValueTest extends Specification {
  class DummyDatabase extends Database {
    var tables = Map.empty[String,Table]
    override def table(name: String): Option[Table] = tables.get(name)
  }

  class DummyNut extends Bolt.Nut(null) {
    private val _database = new DummyDatabase
    override def database: Database = _database

    private var _queryCount = 0
    def queryCount = _queryCount

    override def executeNativeQuery(sql: String): ResultSet = {
      val sb = Struct.newBuilder()
      sb.set("ONE").to(SValue.int64(1))
      sb.set("TWO").to(SValue.int64(2))
      sb.set("THREE").to(SValue.int64(2))

      _queryCount += 1

      ResultSets.forRows(Type.struct(List(Type.StructField.of("ONE",Type.int64()),Type.StructField.of("TWO",Type.int64()),Type.StructField.of("THREE",Type.int64()) )),List(sb.build()))
    }
  }

  "eval" should {
    "$LIKE" in {
      val v = FunctionValueImpl("$LIKE",List(StringValue("testtest"),StringValue("test%"))).eval.asValue
      v.isInstanceOf[BooleanValue] must_== true
      v.asInstanceOf[BooleanValue].f must_== true       
    }
    "$IN" in {
      val vlist = new util.ArrayList[Value]()
      vlist.add(IntValue("1",1,true))
      vlist.add(IntValue("2",2,true))
      vlist.add(IntValue("3",3,true))

      val arr = ArrayValue(vlist,false,Type.int64())
      val v = FunctionValueImpl("$IN",List(IntValue(3),arr)).eval.asValue
      v.isInstanceOf[BooleanValue] must_== true
      v.asInstanceOf[BooleanValue].f must_== true
    }
    "NOT $IN" in {
      val vlist = new util.ArrayList[Value]()
      vlist.add(IntValue("1",1,true))
      vlist.add(IntValue("2",2,true))
      vlist.add(IntValue("3",3,true))

      val arr = ArrayValue(vlist,false,Type.int64())
      val v = FunctionValueImpl("$IN",List(IntValue(4),arr)).eval.asValue
      v.isInstanceOf[BooleanValue] must_== true
      v.asInstanceOf[BooleanValue].f must_== false
    }
    "$BETWEEN" in {
      val v = FunctionValueImpl("$BETWEEN",List(ExpressionValue("+",IntValue(1),IntValue(1)),IntValue(1),IntValue(3))).eval.asValue
      v.isInstanceOf[BooleanValue] must_== true
      v.asInstanceOf[BooleanValue].f must_== true
    }
  }
  "date functions" should {
    "DATE" in {
      val sql = "DATE(2001,1,23)"
      val source = new ByteArrayInputStream(sql.getBytes("UTF8"))
      val input = new ANTLRInputStream(source)
      val lexer = new MiniSqlLexer(input)

      val tokenStream = new CommonTokenStream(lexer)
      val parser = new MiniSqlParser(tokenStream)
      parser.setErrorHandler(new BailErrorStrategy())

      val r = parser.function()
      val v = r.v.eval.asValue.asInstanceOf[DateValue]
      v.getField("YEAR").asInstanceOf[IntValue].value must_== 2001
      v.getField("MONTH").asInstanceOf[IntValue].value must_== 1
      v.getField("DAY").asInstanceOf[IntValue].value must_== 23
    }
    "DATE 2" in {
      val sql = "DATE(TIMESTAMP \"2001-01-23T10:15:30.000Z\")"
      val source = new ByteArrayInputStream(sql.getBytes("UTF8"))
      val input = new ANTLRInputStream(source)
      val lexer = new MiniSqlLexer(input)

      val tokenStream = new CommonTokenStream(lexer)
      val parser = new MiniSqlParser(tokenStream)
      parser.setErrorHandler(new BailErrorStrategy())

      val r = parser.function()
      val v = r.v.eval.asValue.asInstanceOf[DateValue]
      v.getField("YEAR").asInstanceOf[IntValue].value must_== 2001
      v.getField("MONTH").asInstanceOf[IntValue].value must_== 1
      v.getField("DAY").asInstanceOf[IntValue].value must_== 23
    }
    "DATE 3" in {
      val sql = "DATE(TIMESTAMP \"2001-01-23T10:15:30.000Z\",'Asia/Tokyo')"
      val source = new ByteArrayInputStream(sql.getBytes("UTF8"))
      val input = new ANTLRInputStream(source)
      val lexer = new MiniSqlLexer(input)

      val tokenStream = new CommonTokenStream(lexer)
      val parser = new MiniSqlParser(tokenStream)
      parser.setErrorHandler(new BailErrorStrategy())

      val r = parser.function()
      val v = r.v.eval.asValue.asInstanceOf[DateValue]
      v.getField("YEAR").asInstanceOf[IntValue].value must_== 2001
      v.getField("MONTH").asInstanceOf[IntValue].value must_== 1
      v.getField("DAY").asInstanceOf[IntValue].value must_== 23
    }
    "DATE_ADD" in {
      val sql = "DATE_ADD('2001-01-01',INTERVAL 10 DAY)"
      val source = new ByteArrayInputStream(sql.getBytes("UTF8"))
      val input = new ANTLRInputStream(source)
      val lexer = new MiniSqlLexer(input)

      val tokenStream = new CommonTokenStream(lexer)
      val parser = new MiniSqlParser(tokenStream)
      parser.setErrorHandler(new BailErrorStrategy())

      val r = parser.function()
      val v = r.v.eval.asValue.asInstanceOf[DateValue]

      v.getField("YEAR").asInstanceOf[IntValue].value must_== 2001
      v.getField("MONTH").asInstanceOf[IntValue].value must_== 1
      v.getField("DAY").asInstanceOf[IntValue].value must_== 11
    }
    "DATE_ADD 2" in {
      val sql = "DATE_ADD('2001-01-01',INTERVAL 10 WEEK)"
      val source = new ByteArrayInputStream(sql.getBytes("UTF8"))
      val input = new ANTLRInputStream(source)
      val lexer = new MiniSqlLexer(input)

      val tokenStream = new CommonTokenStream(lexer)
      val parser = new MiniSqlParser(tokenStream)
      parser.setErrorHandler(new BailErrorStrategy())

      val r = parser.function()
      val v = r.v.eval.asValue.asInstanceOf[DateValue]

      val d = new DateTime().withDate(2001,1,1).plusWeeks(10)
      
      v.getField("YEAR").asInstanceOf[IntValue].value must_== 2001
      v.getField("MONTH").asInstanceOf[IntValue].value must_== d.getMonthOfYear
      v.getField("DAY").asInstanceOf[IntValue].value must_== d.getDayOfMonth
    }
    "DATE_ADD 3" in {
      val sql = "DATE_ADD('2001-01-01',INTERVAL 10 MONTH)"
      val source = new ByteArrayInputStream(sql.getBytes("UTF8"))
      val input = new ANTLRInputStream(source)
      val lexer = new MiniSqlLexer(input)

      val tokenStream = new CommonTokenStream(lexer)
      val parser = new MiniSqlParser(tokenStream)
      parser.setErrorHandler(new BailErrorStrategy())

      val r = parser.function()
      val v = r.v.eval.asValue.asInstanceOf[DateValue]

      val d = new DateTime().withDate(2001,1,1).plusMonths(10)

      v.getField("YEAR").asInstanceOf[IntValue].value must_== d.getYear
      v.getField("MONTH").asInstanceOf[IntValue].value must_== d.getMonthOfYear
      v.getField("DAY").asInstanceOf[IntValue].value must_== d.getDayOfMonth
    }
    "DATE_ADD 4" in {
      val sql = "DATE_ADD('2001-01-01',INTERVAL 10 QUARTER)"
      val source = new ByteArrayInputStream(sql.getBytes("UTF8"))
      val input = new ANTLRInputStream(source)
      val lexer = new MiniSqlLexer(input)

      val tokenStream = new CommonTokenStream(lexer)
      val parser = new MiniSqlParser(tokenStream)
      parser.setErrorHandler(new BailErrorStrategy())

      val r = parser.function()
      val v = r.v.eval.asValue.asInstanceOf[DateValue]

      val d = new DateTime().withDate(2001,1,1).plusMonths(10 * 3)

      v.getField("YEAR").asInstanceOf[IntValue].value must_== d.getYear
      v.getField("MONTH").asInstanceOf[IntValue].value must_== d.getMonthOfYear
      v.getField("DAY").asInstanceOf[IntValue].value must_== d.getDayOfMonth
    }
    "DATE_ADD 5" in {
      val sql = "DATE_ADD('2001-01-01',INTERVAL 10 YEAR)"
      val source = new ByteArrayInputStream(sql.getBytes("UTF8"))
      val input = new ANTLRInputStream(source)
      val lexer = new MiniSqlLexer(input)

      val tokenStream = new CommonTokenStream(lexer)
      val parser = new MiniSqlParser(tokenStream)
      parser.setErrorHandler(new BailErrorStrategy())

      val r = parser.function()
      val v = r.v.eval.asValue.asInstanceOf[DateValue]

      val d = new DateTime().withDate(2001,1,1).plusYears(10)

      v.getField("YEAR").asInstanceOf[IntValue].value must_== d.getYear
      v.getField("MONTH").asInstanceOf[IntValue].value must_== d.getMonthOfYear
      v.getField("DAY").asInstanceOf[IntValue].value must_== d.getDayOfMonth
    }
    "DATE_SUB" in {
      val sql = "DATE_SUB('2001-01-01',INTERVAL 10 DAY)"
      val source = new ByteArrayInputStream(sql.getBytes("UTF8"))
      val input = new ANTLRInputStream(source)
      val lexer = new MiniSqlLexer(input)

      val tokenStream = new CommonTokenStream(lexer)
      val parser = new MiniSqlParser(tokenStream)
      parser.setErrorHandler(new BailErrorStrategy())

      val r = parser.function()
      val v = r.v.eval.asValue.asInstanceOf[DateValue]

      val d = new DateTime().withDate(2001,1,1).minusDays(10)

      v.getField("YEAR").asInstanceOf[IntValue].value must_== d.getYear
      v.getField("MONTH").asInstanceOf[IntValue].value must_== d.getMonthOfYear
      v.getField("DAY").asInstanceOf[IntValue].value must_== d.getDayOfMonth
    }
    "DATE_SUB 2" in {
      val sql = "DATE_SUB('2001-01-01',INTERVAL 10 WEEK)"
      val source = new ByteArrayInputStream(sql.getBytes("UTF8"))
      val input = new ANTLRInputStream(source)
      val lexer = new MiniSqlLexer(input)

      val tokenStream = new CommonTokenStream(lexer)
      val parser = new MiniSqlParser(tokenStream)
      parser.setErrorHandler(new BailErrorStrategy())

      val r = parser.function()
      val v = r.v.eval.asValue.asInstanceOf[DateValue]

      val d = new DateTime().withDate(2001,1,1).minusWeeks(10)

      v.getField("YEAR").asInstanceOf[IntValue].value must_== d.getYear
      v.getField("MONTH").asInstanceOf[IntValue].value must_== d.getMonthOfYear
      v.getField("DAY").asInstanceOf[IntValue].value must_== d.getDayOfMonth
    }
    "DATE_SUB 3" in {
      val sql = "DATE_SUB('2001-01-01',INTERVAL 10 MONTH)"
      val source = new ByteArrayInputStream(sql.getBytes("UTF8"))
      val input = new ANTLRInputStream(source)
      val lexer = new MiniSqlLexer(input)

      val tokenStream = new CommonTokenStream(lexer)
      val parser = new MiniSqlParser(tokenStream)
      parser.setErrorHandler(new BailErrorStrategy())

      val r = parser.function()
      val v = r.v.eval.asValue.asInstanceOf[DateValue]

      val d = new DateTime().withDate(2001,1,1).minusMonths(10)

      v.getField("YEAR").asInstanceOf[IntValue].value must_== d.getYear
      v.getField("MONTH").asInstanceOf[IntValue].value must_== d.getMonthOfYear
      v.getField("DAY").asInstanceOf[IntValue].value must_== d.getDayOfMonth
    }
    "DATE_SUB 4" in {
      val sql = "DATE_SUB('2001-01-01',INTERVAL 10 QUARTER)"
      val source = new ByteArrayInputStream(sql.getBytes("UTF8"))
      val input = new ANTLRInputStream(source)
      val lexer = new MiniSqlLexer(input)

      val tokenStream = new CommonTokenStream(lexer)
      val parser = new MiniSqlParser(tokenStream)
      parser.setErrorHandler(new BailErrorStrategy())

      val r = parser.function()
      val v = r.v.eval.asValue.asInstanceOf[DateValue]

      val d = new DateTime().withDate(2001,1,1).minusMonths(10 * 3)

      v.getField("YEAR").asInstanceOf[IntValue].value must_== d.getYear
      v.getField("MONTH").asInstanceOf[IntValue].value must_== d.getMonthOfYear
      v.getField("DAY").asInstanceOf[IntValue].value must_== d.getDayOfMonth
    }
    "DATE_SUB 5" in {
      val sql = "DATE_SUB('2001-01-01',INTERVAL 10 YEAR)"
      val source = new ByteArrayInputStream(sql.getBytes("UTF8"))
      val input = new ANTLRInputStream(source)
      val lexer = new MiniSqlLexer(input)

      val tokenStream = new CommonTokenStream(lexer)
      val parser = new MiniSqlParser(tokenStream)
      parser.setErrorHandler(new BailErrorStrategy())

      val r = parser.function()
      val v = r.v.eval.asValue.asInstanceOf[DateValue]

      val d = new DateTime().withDate(2001,1,1).minusYears(10)

      v.getField("YEAR").asInstanceOf[IntValue].value must_== d.getYear
      v.getField("MONTH").asInstanceOf[IntValue].value must_== d.getMonthOfYear
      v.getField("DAY").asInstanceOf[IntValue].value must_== d.getDayOfMonth
    }
    "DATE_DIFF" in {
      val nut = new DummyNut
      val qc = QueryContext(nut,null)

      val sql = "DATE_DIFF(DATE \"2010-07-07\", DATE \"2008-12-25\", DAY)"
      val source = new ByteArrayInputStream(sql.getBytes("UTF8"))
      val input = new ANTLRInputStream(source)
      val lexer = new MiniSqlLexer(input)

      val tokenStream = new CommonTokenStream(lexer)
      val parser = new MiniSqlParser(tokenStream)
      parser.setErrorHandler(new BailErrorStrategy())
      ParserSetter.set(parser,nut,qc)

      val r = parser.function()
      val v = r.v.eval.asValue.asInstanceOf[IntValue]

      v.value must_== 559
    }
    "DATE_TRUNC" in {
      val nut = new DummyNut
      val qc = QueryContext(nut,null)

      val sql = "DATE_TRUNC('2001-01-02',DAY)"
      val source = new ByteArrayInputStream(sql.getBytes("UTF8"))
      val input = new ANTLRInputStream(source)
      val lexer = new MiniSqlLexer(input)

      val tokenStream = new CommonTokenStream(lexer)
      val parser = new MiniSqlParser(tokenStream)
      parser.setErrorHandler(new BailErrorStrategy())
      ParserSetter.set(parser,nut,qc)

      val r = parser.function()
      val v = r.v.eval.asValue.asInstanceOf[DateValue]

      val d = new DateTime().withDate(2001,1,2)

      v.getField("YEAR").asInstanceOf[IntValue].value must_== d.getYear
      v.getField("MONTH").asInstanceOf[IntValue].value must_== d.getMonthOfYear
      v.getField("DAY").asInstanceOf[IntValue].value must_== d.getDayOfMonth
    }
    "DATE_TRUNC 2" in {
      val nut = new DummyNut
      val qc = QueryContext(nut,null)

      val sql = "DATE_TRUNC('2017-08-02',WEEK)"
      val source = new ByteArrayInputStream(sql.getBytes("UTF8"))
      val input = new ANTLRInputStream(source)
      val lexer = new MiniSqlLexer(input)

      val tokenStream = new CommonTokenStream(lexer)
      val parser = new MiniSqlParser(tokenStream)
      parser.setErrorHandler(new BailErrorStrategy())
      ParserSetter.set(parser,nut,qc)

      val r = parser.function()
      val v = r.v.eval.asValue.asInstanceOf[DateValue]

      val d = new DateTime().withDate(2017,7,30)

      v.getField("YEAR").asInstanceOf[IntValue].value must_== d.getYear
      v.getField("MONTH").asInstanceOf[IntValue].value must_== d.getMonthOfYear
      v.getField("DAY").asInstanceOf[IntValue].value must_== d.getDayOfMonth
    }
  }
}

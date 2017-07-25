package com.sopranoworks.bolt

import java.util

import com.google.cloud.spanner._
import com.sopranoworks.bolt.Bolt.Nat
import com.typesafe.config.ConfigFactory
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterEach

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

class SubqueryValueTest extends Specification/* with BeforeAfterEach*/ {
//  sequential
//
//  import Bolt._
//
//  private var _options:Option[SpannerOptions] = None
//  private var _dbClient:Option[DatabaseClient] = None
//
//  override protected def before: Any = {
//    val config = ConfigFactory.load()
//    val options = SpannerOptions.newBuilder().build()
//    _options = Some(options)
//    val spanner = options.getService
//    val dbClient = spanner.getDatabaseClient(DatabaseId.of(options.getProjectId, config.getString("spanner.instance"), config.getString("spanner.database")))
//    Database.startWith(dbClient)
//    _dbClient = Some(dbClient)
//  }
//
//  override protected def after: Any = {
//    _options.foreach(_.getService.close())
//  }
  
  case class DummyNat(res:ResultSet) extends Nat(null) {
    override def executeNativeQuery(sql: String): ResultSet = res
  }

  "asArray" should {
    "1 row and multiple columns" in {
      val sb = Struct.newBuilder()
      sb.add("ONE",Value.int64(1))
      sb.add("TWO",Value.int64(2))
      sb.add("THREE",Value.int64(2))

      val res = ResultSets.forRows(Type.struct(List(Type.StructField.of("ONE",Type.int64()),Type.StructField.of("TWO",Type.int64()),Type.StructField.of("THREE",Type.int64()) )),List(sb.build()))
      val sq = SubqueryValue(DummyNat(res),"",QueryContext(null,null))
      val arr = sq.eval.asArray

      arr.eval.spannerType must_== Type.int64()
      arr.length must_== 3
    }
    "1 column and multiple rows" in {
      val res = ResultSets.forRows(Type.struct(List(Type.StructField.of("ONE",Type.int64()))),
        (0 until 3).map {
          i =>
            val sb = Struct.newBuilder()
            sb.add("ONE",Value.int64(i + 1))
            sb.build()
        }.toList)
      val sq = SubqueryValue(DummyNat(res),"",QueryContext(null,null))
      val arr = sq.eval.asArray

      arr.spannerType must_== Type.int64()
      arr.length must_== 3
    }
    "2 rows and 3 columns" in {
      val res = ResultSets.forRows(Type.struct(List(Type.StructField.of("ONE",Type.int64()),Type.StructField.of("TWO",Type.int64()),Type.StructField.of("THREE",Type.int64()) )),
        (0 until 3).map {
          _ =>
            val sb = Struct.newBuilder()
            sb.add("ONE",Value.int64(1))
            sb.add("TWO",Value.int64(2))
            sb.add("THREE",Value.int64(2))
            sb.build()
        }.toList)
      val sq = SubqueryValue(DummyNat(res),"",QueryContext(null,null))
      sq.eval.asArray must throwA[RuntimeException]
    }
    "1 row and multiple columns including null value" in {
      val sb = Struct.newBuilder()
      sb.add("ONE",Value.int64(1))
      sb.add("TWO",Value.int64(null))
      sb.add("THREE",Value.int64(2))

      val res = ResultSets.forRows(Type.struct(List(Type.StructField.of("ONE",Type.int64()),Type.StructField.of("TWO",Type.int64()),Type.StructField.of("THREE",Type.int64()) )),List(sb.build()))
      val sq = SubqueryValue(DummyNat(res),"",QueryContext(null,null))
      val arr = sq.eval.asArray

      arr.spannerType must_== Type.int64()
      arr.length must_== 2
    }
    "1 column and multiple rows including null value" in {
      val res = ResultSets.forRows(Type.struct(List(Type.StructField.of("ONE",Type.int64()))),
        (0 until 3).map {
          i =>
            val sb = Struct.newBuilder()
            if (i != 1) {
              sb.add("ONE", Value.int64(i + 1))
            } else {
              sb.add("ONE", Value.int64(null))
            }
            sb.build()
        }.toList)
      val sq = SubqueryValue(DummyNat(res),"",QueryContext(null,null))
      val arr = sq.eval.asArray

      arr.spannerType must_== Type.int64()
      arr.length must_== 2
    }
    "0 column and 0 row" in {
      val res = ResultSets.forRows(Type.struct(List(Type.StructField.of("ONE",Type.int64()))),List())
      val sq = SubqueryValue(DummyNat(res),"",QueryContext(null,null))
      val arr = sq.eval.asArray

      arr.spannerType must_== null
      arr.length must_== 0
    }
    "1 row and 1 array column" in {
      val sb = Struct.newBuilder()
      sb.add("ONE",Value.int64Array(Array(1L,2L,3L)))

      val res = ResultSets.forRows(Type.struct(List(Type.StructField.of("ONE",Type.array(Type.int64())) )),List(sb.build()))
      val sq = SubqueryValue(DummyNat(res),"",QueryContext(null,null))
      val arr = sq.eval.asArray

      arr.spannerType must_== Type.int64()
      arr.length must_== 3
    }
    "1 row and 2 array column" in {
      val sb = Struct.newBuilder()
      sb.add("ONE",Value.int64Array(Array(1L,2L,3L)))
      sb.add("TWO",Value.int64Array(Array(1L,2L,3L)))

      val res = ResultSets.forRows(Type.struct(List(Type.StructField.of("ONE",Type.array(Type.int64())),Type.StructField.of("TWO",Type.array(Type.int64())) )),List(sb.build()))
      val sq = SubqueryValue(DummyNat(res),"",QueryContext(null,null))
      sq.eval.asArray must throwA[RuntimeException]
    }
    "1 array column and multiple rows" in {
      val res = ResultSets.forRows(Type.struct(List(Type.StructField.of("ONE",Type.array(Type.int64())))),
        (0 until 3).map {
          i =>
            val sb = Struct.newBuilder()
            sb.add("ONE",Value.int64Array(Array(1L,2L,3L)))
            sb.build()
        }.toList)
      val sq = SubqueryValue(DummyNat(res),"",QueryContext(null,null))
      sq.eval.asArray must throwA[RuntimeException]
    }
  }
}

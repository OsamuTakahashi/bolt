package com.sopranoworks.bolt.values

import com.sopranoworks.bolt._

import com.google.cloud.spanner.{ResultSet, ResultSets, Struct, Type, Value=>SValue, Database => SDatabase}
import org.specs2.mutable.Specification

import scala.collection.JavaConversions._

class ResultIndexValueTest extends Specification {
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
    "from SubqueryValue at index 0" in {
      val nat = new DummyNut
      val qc = QueryContext(nat,null)
      val v = ResultIndexValue(SubqueryValue(nat,"SELECT *",qc),0)
      v.eval.asValue.isInstanceOf[IntValue] must_== true
      v.asValue.asInstanceOf[IntValue].value must_== 1
    }
    "from SubqueryValue at index 1" in {
      val nat = new DummyNut
      val qc = QueryContext(nat,null)
      val v = ResultIndexValue(SubqueryValue(nat,"SELECT *",qc),1)
      v.eval.asValue.isInstanceOf[IntValue] must_== true
      v.asValue.asInstanceOf[IntValue].value must_== 2
    }
    "out of range from SubqueryValue" in {
      val nat = new DummyNut
      val qc = QueryContext(nat,null)
      val v = ResultIndexValue(SubqueryValue(nat,"SELECT *",qc),3)
      v.eval.asValue must throwA[RuntimeException]
    }
    "from StructValue at 0" in {
      val st = StructValue()
      st.addValue(IntValue("1",1,true))
      st.addValue(IntValue("2",2,true))
      st.addValue(IntValue("3",3,true))
      val v = ResultIndexValue(st,0)
      v.eval.asValue.isInstanceOf[IntValue] must_== true
      v.asValue.asInstanceOf[IntValue].value must_== 1
    }
    "from StructValue at 1" in {
      val st = StructValue()
      st.addValue(IntValue("1",1,true))
      st.addValue(IntValue("2",2,true))
      st.addValue(IntValue("3",3,true))
      val v = ResultIndexValue(st,1)
      v.eval.asValue.isInstanceOf[IntValue] must_== true
      v.asValue.asInstanceOf[IntValue].value must_== 2
    }
    "out of range from StructValue" in {
      val st = StructValue()
      st.addValue(IntValue("1",1,true))
      st.addValue(IntValue("2",2,true))
      st.addValue(IntValue("3",3,true))
      val v = ResultIndexValue(st,3)
      v.eval.asValue must throwA[RuntimeException]
    }
    "from Array 0" in {
      val arr = ArrayValue(List(IntValue("1",1,true),IntValue("2",2,true),IntValue("3",3,true)),true,Type.int64())
      val v = ResultIndexValue(arr,0)
      v.eval.asValue.isInstanceOf[IntValue] must_== true
      v.asValue.asInstanceOf[IntValue].value must_== 1
    }
    "from Array 1" in {
      val arr = ArrayValue(List(IntValue("1",1,true),IntValue("2",2,true),IntValue("3",3,true)),true,Type.int64())
      val v = ResultIndexValue(arr,1)
      v.eval.asValue.isInstanceOf[IntValue] must_== true
      v.asValue.asInstanceOf[IntValue].value must_== 2
    }
    "out of range from Array" in {
      val arr = ArrayValue(List(IntValue("1",1,true),IntValue("2",2,true),IntValue("3",3,true)),true,Type.int64())
      val v = ResultIndexValue(arr,3)
      v.eval.asValue must throwA[RuntimeException]
    }
  }
}

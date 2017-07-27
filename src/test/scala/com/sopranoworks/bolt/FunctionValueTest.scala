package com.sopranoworks.bolt

import java.util

import com.google.cloud.spanner.Type
import org.specs2.mutable.Specification

import scala.collection.JavaConversions._

class FunctionValueTest extends Specification {
  "eval" should {
    "$LIKE" in {
      val v = FunctionValue("$LIKE",List(StringValue("testtest"),StringValue("test%"))).eval.asValue
      v.isInstanceOf[BooleanValue] must_== true
      v.asInstanceOf[BooleanValue].f must_== true       
    }
    "$IN" in {
      val vlist = new util.ArrayList[Value]()
      vlist.add(IntValue("1",1,true))
      vlist.add(IntValue("2",2,true))
      vlist.add(IntValue("3",3,true))

      val arr = ArrayValue(vlist,false,Type.int64())
      val v = FunctionValue("$IN",List(IntValue(3),arr)).eval.asValue
      v.isInstanceOf[BooleanValue] must_== true
      v.asInstanceOf[BooleanValue].f must_== true
    }
    "NOT $IN" in {
      val vlist = new util.ArrayList[Value]()
      vlist.add(IntValue("1",1,true))
      vlist.add(IntValue("2",2,true))
      vlist.add(IntValue("3",3,true))

      val arr = ArrayValue(vlist,false,Type.int64())
      val v = FunctionValue("$IN",List(IntValue(4),arr)).eval.asValue
      v.isInstanceOf[BooleanValue] must_== true
      v.asInstanceOf[BooleanValue].f must_== false
    }
    "$BETWEEN" in {
      val v = FunctionValue("$BETWEEN",List(ExpressionValue("+",IntValue(1),IntValue(1)),IntValue(1),IntValue(3))).eval.asValue
      v.isInstanceOf[BooleanValue] must_== true
      v.asInstanceOf[BooleanValue].f must_== true
    }
  }
}

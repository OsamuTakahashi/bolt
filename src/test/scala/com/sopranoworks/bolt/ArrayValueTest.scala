package com.sopranoworks.bolt

import java.util

import com.google.cloud.spanner.Type
import org.specs2.mutable.Specification

class ArrayValueTest extends Specification {
  "eval" should {
    "same elements" in {
      val vlist = new util.ArrayList[Value]()
      vlist.add(IntValue("1",1,true))
      vlist.add(IntValue("2",2,true))
      vlist.add(IntValue("3",3,true))

      val arr = ArrayValue(vlist)
      val arr2 = arr.eval.asValue
      arr2.isInstanceOf[ArrayValue] must_== true
      arr2.spannerType must_== Type.int64()
    }
    "same elements and use arrayType" in {
      val vlist = new util.ArrayList[Value]()
      vlist.add(IntValue("1",1,true))
      vlist.add(IntValue("2",2,true))
      vlist.add(IntValue("3",3,true))

      val arr = ArrayValue(vlist,false,Type.int64())
      val arr2 = arr.eval.asValue
      arr2.isInstanceOf[ArrayValue] must_== true
      arr2.spannerType must_== Type.int64()
    }
    "invalid values" in {
      val vlist = new util.ArrayList[Value]()
      vlist.add(IntValue("1",1,true))
      vlist.add(StringValue("2"))
      vlist.add(IntValue("3",3,true))

      val arr = ArrayValue(vlist)
      arr.eval.asValue must throwA[RuntimeException]
    }
    "0 element" in {
      val vlist = new util.ArrayList[Value]()
      val arr = ArrayValue(vlist)
      val arr2 = arr.eval.asValue
      arr2.isInstanceOf[ArrayValue] must_== true
    }
    "0 element with type" in {
      val vlist = new util.ArrayList[Value]()
      val arr = ArrayValue(vlist,false,Type.int64())
      val arr2 = arr.eval.asValue
      arr2.isInstanceOf[ArrayValue] must_== true
      arr2.spannerType must_== Type.int64()
    }
    "contains null" in {
      val vlist = new util.ArrayList[Value]()
      vlist.add(IntValue("1",1,true))
      vlist.add(NullValue)
      vlist.add(IntValue("3",3,true))

      val arr = ArrayValue(vlist)
      val arr2 = arr.eval.asValue.asInstanceOf[ArrayValue]
      arr2.isInstanceOf[ArrayValue] must_== true
      arr2.spannerType must_== Type.int64()
      arr2.length must_== 2
    }
  }
}

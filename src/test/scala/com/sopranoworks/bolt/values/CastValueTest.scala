package com.sopranoworks.bolt.values

import java.util

import com.google.cloud.spanner.Type
import org.specs2.mutable.Specification


class CastValueTest extends Specification {
  "to bool" should {
    "from bool" in {
      val ex = CastValue(BooleanValue(true),Type.bool())
      ex.eval.asValue.isInstanceOf[BooleanValue] must_== true
      ex.asValue.asInstanceOf[BooleanValue].f must_== true
    }
    "from int" in {
      val ex = CastValue(IntValue(1),Type.bool())
      ex.eval.asValue.isInstanceOf[BooleanValue] must_== true
      ex.asValue.asInstanceOf[BooleanValue].f must_== true
    }
    "from string" in {
      val ex = CastValue(StringValue("true"),Type.bool())
      ex.eval.asValue.isInstanceOf[BooleanValue] must_== true
      ex.asValue.asInstanceOf[BooleanValue].f must_== true
    }
    "from double" in {
      val ex = CastValue(DoubleValue(0),Type.bool())
      ex.eval must throwA[RuntimeException]
    }
  }
  "to int64" should {
    "from bool" in {
      val ex = CastValue(BooleanValue(true),Type.int64())
      ex.eval.asValue.isInstanceOf[IntValue] must_== true
      ex.asValue.asInstanceOf[IntValue].value must_== 1
    }
    "from int" in {
      val ex = CastValue(IntValue(2),Type.int64())
      ex.eval.asValue.isInstanceOf[IntValue] must_== true
      ex.asValue.asInstanceOf[IntValue].value must_== 2
    }
    "from double" in {
      val ex = CastValue(DoubleValue(1.1),Type.int64())
      ex.eval.asValue.isInstanceOf[IntValue] must_== true
      ex.asValue.asInstanceOf[IntValue].value must_== 1
    }
    "from string" in {
      val ex = CastValue(StringValue("100"),Type.int64())
      ex.eval.asValue.isInstanceOf[IntValue] must_== true
      ex.asValue.asInstanceOf[IntValue].value must_== 100
    }
    "from datetime" in {
      val ex = CastValue(FunctionValue("NOW",new util.ArrayList[Value]()),Type.int64())
      ex.eval must throwA[RuntimeException]
    }
  }
  "to float64" should {
    "from int" in {
      val ex = CastValue(IntValue(1),Type.float64())
      ex.eval.asValue.isInstanceOf[DoubleValue] must_== true
      ex.asValue.asInstanceOf[DoubleValue].value must_== 1.toDouble
    }
    "from double" in {
      val ex = CastValue(DoubleValue(1.1),Type.float64())
      ex.eval.asValue.isInstanceOf[DoubleValue] must_== true
      ex.asValue.asInstanceOf[DoubleValue].value must_== 1.1
    }
    "from string" in {
      val ex = CastValue(StringValue("1.2"),Type.float64())
      ex.eval.asValue.isInstanceOf[DoubleValue] must_== true
      ex.asValue.asInstanceOf[DoubleValue].value must_== 1.2
    }
    "from bool" in {
      val ex = CastValue(BooleanValue(true),Type.float64())
      ex.eval must throwA[RuntimeException]
    }
  }
  "te string" should {
    "from bool" in {
      val v = BooleanValue(true)
      val ex = CastValue(v,Type.string())
      ex.eval.asValue.isInstanceOf[StringValue] must_== true
      ex.asValue.asInstanceOf[StringValue].text must_== v.text
    }
    "from int" in {
      val v = IntValue(3)
      val ex = CastValue(v,Type.string())
      ex.eval.asValue.isInstanceOf[StringValue] must_== true
      ex.asValue.asInstanceOf[StringValue].text must_== v.text
    }
    "from float" in {
      val v = DoubleValue(1.5)
      val ex = CastValue(v,Type.string())
      ex.eval.asValue.isInstanceOf[StringValue] must_== true
      ex.asValue.asInstanceOf[StringValue].text must_== v.text
    }
    "from string" in {
      val v = StringValue("test")
      val ex = CastValue(v,Type.string())
      ex.eval.asValue.isInstanceOf[StringValue] must_== true
      ex.asValue.asInstanceOf[StringValue].text must_== v.text
    }
  }
}

package com.sopranoworks.bolt.values

import org.specs2.mutable.Specification

class DateValueTest extends Specification {

  "evak" should {
    "normally success" in {
      val v = DateValue("2001-01-01")
      v.eval
      v.getField("YEAR").asInstanceOf[IntValue].value must_== 2001
      v.getField("MONTH").asInstanceOf[IntValue].value must_== 1
      v.getField("DAY").asInstanceOf[IntValue].value must_== 1
    }
    "invalid format" in {
      val v = DateValue("2001-12-34")
      v.eval must throwA[java.lang.IllegalArgumentException]
    }
  }
}

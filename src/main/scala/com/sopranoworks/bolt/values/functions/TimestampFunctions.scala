/**
  * Bolt
  * TimestampFunctions
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt.values.functions

import com.google.cloud.Timestamp
import com.sopranoworks.bolt.values.{FunctionValue, TimestampValue, Value}

import scala.collection.JavaConversions._

trait TimestampFunctions { _ : FunctionValue =>
  val timestampFunctions : PartialFunction[String,Option[Value]] = {
    case "NOW" =>
      if (parameters.nonEmpty)
        throw new RuntimeException("Function NOW takes no parameter")
      val ts = Timestamp.now()
      Some(TimestampValue(ts.toString))
  }
}

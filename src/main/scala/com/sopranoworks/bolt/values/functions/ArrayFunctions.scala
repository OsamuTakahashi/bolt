/**
  * Bolt
  * ArrayFunctions
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt.values.functions

import com.sopranoworks.bolt.values.{ArrayValue, FunctionValue, IntValue, Value}

import scala.collection.JavaConversions._

trait ArrayFunctions { _ : FunctionValue =>
  val arrayFunctions : PartialFunction[String,Option[Value]] = {
    case "ARRAY_LENGTH" =>
      if (parameters.length != 1)
        throw new RuntimeException("Function ARRAY_LENGTH takes 1 parameter")

      _checkStayUnresolved {
        parameters.get(0).eval.asValue match {
          case arr: ArrayValue =>
            val l = arr.length
            IntValue(l.toString, l, true)
          case _ =>
            throw new RuntimeException("The parameter type of ARRAY_LENGTH must be ARRAY")
        }
      }
  }
}

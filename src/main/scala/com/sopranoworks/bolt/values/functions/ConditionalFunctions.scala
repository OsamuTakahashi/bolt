/**
  * Bolt
  * ConditionalFunctions
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt.values.functions

import com.sopranoworks.bolt.values.{BooleanValue, FunctionValue, NullValue, Value}

import scala.collection.JavaConversions._

trait ConditionalFunctions { _ : FunctionValue =>
  val conditionalFunctions : PartialFunction[String,Option[Value]] = {
    case "COALESCE" =>
      if (parameters.isEmpty)
        throw new RuntimeException("Function COALESCE takes least 1 parameter")

      _checkStayUnresolved {
        parameters.find(_.eval.asValue != NullValue).getOrElse(NullValue)
      }
    case "IF" =>
      if (parameters.length != 3)
        throw new RuntimeException("Function IF takes 3 parameters")
      _checkStayUnresolved {
        val cond = parameters.get(0).eval.asValue
        cond match {
          case BooleanValue(f) =>
            (if (f) parameters.get(1) else parameters(2)).eval.asValue
          case _ =>
//              if (!cond.isBoolean)
              throw new RuntimeException("The first parameter of function IF must be boolean expression")
        }
      }
    case "IFNULL" =>
      if (parameters.length != 2)
        throw new RuntimeException("Function IF takes 2 parameters")
      _checkStayUnresolved {
        parameters.get(0).eval.asValue match {
          case NullValue =>
            parameters.get(1)
          case v =>
            v
        }
      }
  }
}

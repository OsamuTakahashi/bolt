/**
  * Bolt
  * SystemFunctions
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt.values.functions

import com.sopranoworks.bolt.LikeMatcher
import com.sopranoworks.bolt.values._

import scala.collection.JavaConversions._

trait SystemFunctions { _ : FunctionValue =>
  val systemFunctions : PartialFunction[String,Option[Value]] = {
    case "$ARRAY" =>
      if (parameters.length != 1)
        throw new RuntimeException("Function $ARRAY takes 1 parameter")

      _checkStayUnresolved {
        parameters.head match {
          case v: ArrayValue =>
            v
          case q: SubqueryValue =>
            q.eval.asArray
          case _ =>
            throw new RuntimeException("Could not convert parameters to an array")
        }
      }

    case "$OFFSET" =>
      if (parameters.length != 2)
        throw new RuntimeException("Function $OFFSET takes 1 parameter")
      _checkStayUnresolved {
        (parameters.get(0).eval.asValue, parameters.get(1).eval.asValue) match {
          case (arr: ArrayValue, i: IntValue) =>
            arr.offset(i)
          case (_, _: IntValue) =>
            throw new RuntimeException("Array element access with non-array value")
          case (_: ArrayValue, _) =>
            throw new RuntimeException("Index type of array element access must be INT64")
          case (_, _) =>
            throw new RuntimeException("Invalid operation")
        }
      }

    case "$ORDINAL" =>
      if (parameters.length != 2)
        throw new RuntimeException("Function $ORDINAL takes 1 parameter")
      _checkStayUnresolved {
        (parameters.get(0).eval.asValue, parameters.get(1).eval.asValue) match {
          case (arr: ArrayValue, i: IntValue) =>
            arr.ordinal(i)
          case (_, _: IntValue) =>
            throw new RuntimeException("Array element access with non-array value")
          case (_: ArrayValue, _) =>
            throw new RuntimeException("Index type of array element access must be INT64")
          case (_, _) =>
            throw new RuntimeException("Invalid operation")
        }
      }

    case "$EXISTS" =>
      if (parameters.length != 1)
        throw new RuntimeException("Function $EXISTS takes 1 parameter")
      _checkStayUnresolved {
        parameters.get(0).eval match {
          case q: SubqueryValue =>
            BooleanValue(q.numRows > 0)
          case _ =>
            throw new RuntimeException("Invalid operation at EXISTS")
        }
      }

    case "$LIKE" =>
      if (parameters.length != 2)
        throw new RuntimeException("Function $LIKE takes 2 parameter")
      _checkStayUnresolved {
        (parameters.get(0).eval.asValue, parameters.get(1).eval.asValue) match {
          case (a: StringValue, b: StringValue) =>
            BooleanValue(LikeMatcher(b.text).findFirstIn(a.text).isDefined)

          case (_, _) =>
            throw new RuntimeException("Invalid operation at LIKE")
        }
      }

    case "$IN" =>
      if (parameters.length != 2)
        throw new RuntimeException("Function $IN takes 2 parameter")
      _checkStayUnresolved {
        (parameters.get(0).eval.asValue, parameters.get(1).eval.asArray) match {
          case (v, arr: ArrayValue) =>
            BooleanValue(if (arr.length > 0 && v.spannerType == arr.spannerType && v.spannerType != null) {
              arr.contains(v)
            } else false)

          case (_, _) =>
            throw new RuntimeException("Invalid operation at LIKE")
        }
      }

    case "$BETWEEN" =>
      if (parameters.length != 3)
        throw new RuntimeException("Function $BETWEEN takes 3 parameter")
      _checkStayUnresolved {
        val v = parameters.get(0).eval.asValue
        val a = parameters.get(1).eval.asValue
        val b = parameters.get(2).eval.asValue

        try {
          BooleanExpressionValue("AND", BooleanExpressionValue("<=", a, v), BooleanExpressionValue("<=", v, b)).eval.asValue
        } catch {
          case _: RuntimeException =>
            throw new RuntimeException("Invalid operation in BETWEEN")
        }
      }
  }
}

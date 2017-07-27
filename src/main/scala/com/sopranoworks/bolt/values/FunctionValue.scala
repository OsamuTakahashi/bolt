/**
  * Bolt
  * FunctionValue
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt.values

import com.google.cloud.Timestamp
import com.sopranoworks.bolt.LikeMatcher

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
  *
  */
case class FunctionValue(name:String,parameters:java.util.List[Value]) extends WrappedValue {

  override def text = s"$name(${parameters.map(_.text).mkString(",")})"
  override def eval: Value = {
    name match {
      case "NOW" =>
        if (parameters.nonEmpty)
          throw new RuntimeException("Function NOW takes no parameter")
        val ts = Timestamp.now()
        _ref = Some(TimestampValue(ts.toString))

      case "COALESCE" =>
        if (parameters.isEmpty)
          throw new RuntimeException("Function COALESCE takes least 1 parameter")
        _ref = parameters.find(_.eval.asValue != NullValue).orElse(Some(NullValue))
      case "IF" =>
        if (parameters.length != 3)
          throw new RuntimeException("Function IF takes 3 parameters")
        val cond = parameters.get(0).eval.asValue
        cond match {
          case BooleanValue(f) =>
            _ref = Some((if (f) parameters.get(1) else parameters(2)).eval.asValue)
          case _ =>
            if (!cond.isBoolean)
              throw new RuntimeException("The first parameter of function IF must be boolean expression")
        }
      case "IFNULL" =>
        if (parameters.length != 2)
          throw new RuntimeException("Function IF takes 2 parameters")
        parameters.get(0).eval.asValue match {
          case NullValue =>
            _ref = Some(parameters.get(1))
          case v =>
            _ref = Some(v)
        }

        // Array functions
      case "ARRAY_LENGTH" =>
        if (parameters.length != 1)
          throw new RuntimeException("Function ARRAY_LENGTH takes 1 parameter")

        parameters.get(0).eval.asValue match {
          case arr:ArrayValue =>
            val l = arr.length
            _ref = Some(IntValue(l.toString,l,true))
          case _ =>
            throw new RuntimeException("The parameter type of ARRAY_LENGTH must be ARRAY")
        }


        // internal functions
      case "$ARRAY" =>
        if (parameters.length != 1)
          throw new RuntimeException("Function $ARRAY takes 1 parameter")

        _ref = Some(parameters.head match {
          case v:ArrayValue =>
            v
          case q:SubqueryValue =>
            q.eval.asArray
          case _ =>
            throw new RuntimeException("Could not convert parameters to an array")
        })

      case "$OFFSET" =>
        if (parameters.length != 2)
          throw new RuntimeException("Function $OFFSET takes 1 parameter")
        (parameters.get(0).eval.asValue,parameters.get(1).eval.asValue) match {
          case (arr:ArrayValue,i:IntValue) =>
            _ref = Some(arr.offset(i))
          case (_,_:IntValue) =>
            throw new RuntimeException("Array element access with non-array value")
          case (_:ArrayValue,_) =>
            throw new RuntimeException("Index type of array element access must be INT64")
          case (_,_) =>
            throw new RuntimeException("Invalid operation")
        }

      case "$ORDINAL" =>
        if (parameters.length != 2)
          throw new RuntimeException("Function $ORDINAL takes 1 parameter")
        (parameters.get(0).eval.asValue,parameters.get(1).eval.asValue) match {
          case (arr:ArrayValue,i:IntValue) =>
            _ref = Some(arr.ordinal(i))
          case (_,_:IntValue) =>
            throw new RuntimeException("Array element access with non-array value")
          case (_:ArrayValue,_) =>
            throw new RuntimeException("Index type of array element access must be INT64")
          case (_,_) =>
            throw new RuntimeException("Invalid operation")
        }
      case "$EXISTS" =>
        if (parameters.length != 1)
          throw new RuntimeException("Function $EXISTS takes 1 parameter")
        parameters.get(0).eval match {
          case q : SubqueryValue =>
            _ref = Some(BooleanValue(q.numRows > 0))
          case _ =>
            throw new RuntimeException("Invalid operation at EXISTS")
        }

      case "$LIKE" =>
        if (parameters.length != 2)
          throw new RuntimeException("Function $LIKE takes 2 parameter")
        (parameters.get(0).eval.asValue,parameters.get(1).eval.asValue) match {
          case (a:StringValue,b:StringValue) =>
            _ref = Some(BooleanValue(LikeMatcher(b.text).findFirstIn(a.text).isDefined))

          case (_,_) =>
            throw new RuntimeException("Invalid operation at LIKE")
        }

      case "$IN" =>
        if (parameters.length != 2)
          throw new RuntimeException("Function $IN takes 2 parameter")
        (parameters.get(0).eval.asValue,parameters.get(1).eval.asArray) match {
          case (v,arr:ArrayValue) =>
            _ref = Some(BooleanValue(if (arr.length > 0 && v.spannerType == arr.spannerType && v.spannerType != null) {
              arr.contains(v)
            } else false))

          case (_,_) =>
            throw new RuntimeException("Invalid operation at LIKE")
        }

      case "$BETWEEN" =>
        if (parameters.length != 3)
          throw new RuntimeException("Function $BETWEEN takes 3 parameter")
        val v = parameters.get(0).eval.asValue
        val a = parameters.get(1).eval.asValue
        val b = parameters.get(2).eval.asValue

        try {
          _ref = Some(BooleanExpressionValue("&&", BooleanExpressionValue("<=", a, v), BooleanExpressionValue("<=", v, b)).eval.asValue)
        } catch {
          case _ : RuntimeException =>
            throw new RuntimeException("Invalid operation in BETWEEN")
        }

      case _ =>
        throw new RuntimeException(s"Unevaluatable function $name")
    }
    this
  }

  override def invalidateEvaluatedValueIfContains(values:List[Value]):Boolean = {
    if (parameters.foldLeft(false) {
      case (f,v) =>
        f || v.invalidateEvaluatedValueIfContains(values)
    }) {
      _ref = None
      true
    } else false
  }


  override def resolveReference(columns:Map[String,TableColumnValue]): Map[String,TableColumnValue] = {
    parameters.foldLeft(columns){
      case (c,p) =>
        p.resolveReference(c)
    }
  }
}
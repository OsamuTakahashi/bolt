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

import com.sopranoworks.bolt.values.functions._

import scala.collection.JavaConversions._

trait FunctionValue extends WrappedValue {
  protected def _checkStayUnresolved(f: => Value):Option[Value] = {
    _stayUnresolved = parameters.foldLeft(false) {
      case (ck,v) =>
        ck || v.eval.stayUnresolved
    }
    if (!_stayUnresolved) Some(f) else None
  }

  def name:String
  def parameters:java.util.List[Value]
}

/**
  *
  */
case class FunctionValueImpl(name:String,parameters:java.util.List[Value]) extends FunctionValue
  with ArrayFunctions
  with DateFunctions
  with TimestampFunctions
  with ConditionalFunctions
  with SystemFunctions
{
  private val _defaultFunction : PartialFunction[String,Option[Value]] = {
    case _ =>
      throw new RuntimeException(s"Unknown function $name")
  }

  private val _functions = arrayFunctions orElse dateFunctions orElse timestampFunctions orElse conditionalFunctions orElse systemFunctions orElse _defaultFunction

  override def text = s"$name(${parameters.map(_.text).mkString(",")})"
  override def eval: Value = {
    _ref = _functions(name)
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
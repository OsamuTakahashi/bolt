/**
  * Bolt
  * StructValue
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt.values

import com.google.cloud.spanner.Type

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
  *
  */
case class StructValue() extends Value {
  private var _evaluated = false
  private var _members:List[Value] = Nil
  private var _aliases = Map.empty[String,Int]

  override def text = s"STRUCT(${_members.map(_.text).mkString(",")})"

  override def eval: Value = {
    if (!_evaluated) {
      _members = _members.map(_.eval.asValue)
      _evaluated = true
    }
    this
  }

  def addValue(v:Value):Unit = {
    _members :+= v
  }
  def addFieldName(name:String,idx:Int):Unit = {
    if (_aliases.contains(name))
      throw new RuntimeException(s"Duplicate field name $name")
    _aliases += (name->idx)
  }

  override def getField(fieldIdx: Int): Value = {
    if (fieldIdx < 0 || _members.length <= fieldIdx)
      throw new RuntimeException("THe specified filed index is out of range")
    _members(fieldIdx)
  }

  override def getField(fieldName: String): Value = {
    _aliases.get(fieldName) match {
      case Some(idx) => getField(idx)
      case None => throw new RuntimeException(s"The struct does not have field named '$fieldName'")
    }
  }

  private def _isArrayType(t:Type):Boolean =
    t match {
      case v if v == Type.array(Type.bool()) =>
        true
      case v if v == Type.array(Type.int64()) =>
        true
      case v if v == Type.array(Type.float64()) =>
        true
      case v if v == Type.array(Type.string()) =>
        true
      case v if v == Type.array(Type.timestamp()) =>
        true
      case v if v == Type.array(Type.date()) =>
        true
      case _ =>
        false
    }

  override def asArray: ArrayValue = {
    this.eval
    _members match {
      case Nil =>
        ArrayValue(List())
      case head :: tail =>
        if (!_isArrayType(head.spannerType) || !_members.forall(_.spannerType == head.spannerType))
          throw new RuntimeException("The struct can not convert to an array")
        ArrayValue(_members,true,head.spannerType)
    }
  }

  override def invalidateEvaluatedValueIfContains(values:List[Value]):Boolean = {
    _members.foldLeft(false) {
      case (f,v) => f || v.invalidateEvaluatedValueIfContains(values)
    }
  }
}
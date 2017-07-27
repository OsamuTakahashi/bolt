/**
  * Bolt
  * ArrayValue
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt.values

import com.google.cloud.spanner.{Mutation, Type}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

case class ArrayValue(var values:java.util.List[Value],var evaluated:Boolean = false,var arrayType:Type = null) extends Value  {
  private var _evaluated = List.empty[Value]

  private def _isValidArray:Boolean = {
    if (_evaluated.isEmpty) {
      true
    } else {
      val t = if (arrayType != null) arrayType else _evaluated(0).spannerType
      if (t != null) {
        arrayType = t
        _evaluated.forall(_.spannerType == t)
      } else {
        false
      }
    }
  }

  override def eval: Value = {
    if (!evaluated || values.length != _evaluated.length) {
      _evaluated = values.toList.map(_.eval.asValue).filter(_ != NullValue)
      if (!_isValidArray)
        throw new RuntimeException("")
      evaluated = true
    }
    this
  }

  override def invalidateEvaluatedValueIfContains(values: List[Value]): Boolean = {
    if (values.foldLeft(false) {
      case (f,v) => f || v.invalidateEvaluatedValueIfContains(values)
    }) {
      evaluated = false
      _evaluated = List.empty[Value]
      true
    } else false
  }

  override def resolveReference(columns: Map[String, TableColumnValue]): Map[String, TableColumnValue] = {
    values.foldLeft(columns) {
      case (c,v) =>
        v.resolveReference(c)
    }
  }

  override def text:String = values.map(_.text).mkString("[",",","]")
  override def spannerType: Type = {
    eval
    arrayType
  }

  override def asArray: ArrayValue = this

  def length:Int = {
    eval
    _evaluated.length
  }

  def offset(v:Value):Value = {
    val n = v.eval.asValue match {
      case NullValue =>
        0
      case IntValue(_,i,_) =>
        i.toInt
      case _ =>
        throw new RuntimeException("Array offset type must be int64")
    }
    getField(n)
  }

  def ordinal(v:Value):Value = {
    val n = v.eval.asValue match {
      case NullValue =>
        0
      case IntValue(_,i,_) =>
        i.toInt - 1
      case _ =>
        throw new RuntimeException("Array offset type must be int64")
    }
    getField(n)
  }

  def contains(v:Value):Boolean = {
    this.eval
    _evaluated.exists(_.isEqualValue(v))
  }

  override def setTo(m: Mutation.WriteBuilder, key: String): Unit = {
    this.eval
    m.set(key).toStringArray(values.map(_.text))
  }

  override def getField(fieldIdx:Int):Value = {
    if (fieldIdx < 0 || values.length <= fieldIdx) throw new ArrayIndexOutOfBoundsException
    values.get(fieldIdx)
  }
}
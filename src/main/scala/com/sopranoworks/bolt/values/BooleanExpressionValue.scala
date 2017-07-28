/**
  * Bolt
  * BooleanExpressionValue
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt.values

/**
  *
  * @param op boolean operator
  * @param left left expression value
  * @param right right expression value
  */
case class BooleanExpressionValue(op:String,left:Value,right:Value) extends WrappedValue {
  override def text = s"(${left.text} $op ${right.text})"
  override def isBoolean:Boolean = true

  override def eval: Value = {
    if (_ref.isEmpty) {
      val l = left.eval.stayUnresolved
      val r = if (right != null) right.eval.stayUnresolved else false

      _stayUnresolved = l || r
      if (_stayUnresolved) {
        return this
      }

      (op, left.eval.asValue, if (right != null) right.eval.asValue else right) match {
        case ("<",l:IntValue,r:IntValue) =>
          _ref = Some(BooleanValue(l.value < r.value))
        case ("<",l:DoubleValue,r:DoubleValue) =>
          _ref = Some(BooleanValue(l.value < r.value))
        case ("<",l:IntValue,r:DoubleValue) =>
          _ref = Some(BooleanValue(l.value < r.value))
        case ("<",l:DoubleValue,r:IntValue) =>
          _ref = Some(BooleanValue(l.value < r.value))

        case (">",l:IntValue,r:IntValue) =>
          _ref = Some(BooleanValue(l.value > r.value))
        case (">",l:DoubleValue,r:DoubleValue) =>
          _ref = Some(BooleanValue(l.value > r.value))
        case (">",l:IntValue,r:DoubleValue) =>
          _ref = Some(BooleanValue(l.value > r.value))
        case (">",l:DoubleValue,r:IntValue) =>
          _ref = Some(BooleanValue(l.value > r.value))

        case ("<=",l:IntValue,r:IntValue) =>
          _ref = Some(BooleanValue(l.value <= r.value))
        case ("<=",l:DoubleValue,r:DoubleValue) =>
          _ref = Some(BooleanValue(l.value <= r.value))
        case ("<=",l:IntValue,r:DoubleValue) =>
          _ref = Some(BooleanValue(l.value <= r.value))
        case ("<=",l:DoubleValue,r:IntValue) =>
          _ref = Some(BooleanValue(l.value <= r.value))

        case (">=",l:IntValue,r:IntValue) =>
          _ref = Some(BooleanValue(l.value >= r.value))
        case (">=",l:DoubleValue,r:DoubleValue) =>
          _ref = Some(BooleanValue(l.value >= r.value))
        case (">=",l:IntValue,r:DoubleValue) =>
          _ref = Some(BooleanValue(l.value >= r.value))
        case (">=",l:DoubleValue,r:IntValue) =>
          _ref = Some(BooleanValue(l.value >= r.value))

        case ("=",l:IntValue,r:IntValue) =>
          _ref = Some(BooleanValue(l.value == r.value))
        case ("=",l:DoubleValue,r:DoubleValue) =>
          _ref = Some(BooleanValue(l.value == r.value))
        case ("=",l:IntValue,r:DoubleValue) =>
          _ref = Some(BooleanValue(l.value == r.value))
        case ("=",l:DoubleValue,r:IntValue) =>
          _ref = Some(BooleanValue(l.value == r.value))

        case ("!=",l:IntValue,r:IntValue) =>
          _ref = Some(BooleanValue(l.value != r.value))
        case ("!=",l:DoubleValue,r:DoubleValue) =>
          _ref = Some(BooleanValue(l.value != r.value))
        case ("!=",l:IntValue,r:DoubleValue) =>
          _ref = Some(BooleanValue(l.value != r.value))
        case ("!=",l:DoubleValue,r:IntValue) =>
          _ref = Some(BooleanValue(l.value != r.value))


        case ("AND",l:BooleanValue,r:BooleanValue) =>
          _ref = Some(BooleanValue(l.f && r.f))
        case ("OR",l:BooleanValue,r:BooleanValue) =>
          _ref = Some(BooleanValue(l.f || r.f))
        case ("XOR",l:BooleanValue,r:BooleanValue) =>
          _ref = Some(BooleanValue(l.f ^ r.f))
        case ("!",l:BooleanValue,_) =>
          _ref = Some(BooleanValue(!l.f))

        case _ =>
          throw new RuntimeException(s"Invalid operator between the types:$op")
      }
    }
    this
  }
  override def invalidateEvaluatedValueIfContains(values:List[Value]):Boolean = {
    if ((if (left != null) left.invalidateEvaluatedValueIfContains(values) else false) ||
      (if (right != null) right.invalidateEvaluatedValueIfContains(values) else false)) {
      _ref = None
      true
    } else false
  }

  override def resolveReference(columns:Map[String,TableColumnValue]): Map[String,TableColumnValue] = {
    val lc = if (left != null) left.resolveReference(columns) else columns
    if (right != null) right.resolveReference(lc) else lc
  }
}
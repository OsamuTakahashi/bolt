/**
  * Bolt
  * ExpressionValue
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
  * @param op operator
  * @param left left expression value
  * @param right right expression value
  */
case class ExpressionValue(op:String,left:Value,right:Value) extends WrappedValue {
  override def text = s"(${left.text} $op ${right.text})"
  override def eval: Value = {
    if (_ref.isEmpty) {
      (op, left.eval.asValue, if (right != null) right.eval.asValue else right) match {
        case ("-", l:IntValue, null) =>
          val v = -l.value
          _ref = Some(IntValue(v.toString,v,true))
        case ("-", l:DoubleValue, null) =>
          val v = -l.value
          _ref = Some(DoubleValue(v.toString,v,true))
        case ("~", l:IntValue, null) =>
          val v = ~l.value
          _ref = Some(IntValue(v.toString,v,true))

        case ("+", l:IntValue, r:IntValue) =>
          val res = l.value + r.value
          _ref = Some(IntValue(res.toString,res,true))
        case ("+", l:DoubleValue, r:DoubleValue) =>
          val res = l.value + r.value
          _ref = Some(DoubleValue(res.toString,res,true))
        case ("+", l:IntValue, r:DoubleValue) =>
          val res = l.value + r.value
          _ref = Some(DoubleValue(res.toString,res,true))
        case ("+", l:DoubleValue, r:IntValue) =>
          val res = l.value + r.value
          _ref = Some(DoubleValue(res.toString,res,true))
        case ("+", NullValue, _) | ("+", _, NullValue) =>
          _ref = Some(NullValue)

        case ("-", l:IntValue, r:IntValue) =>
          val res = l.value - r.value
          _ref = Some(IntValue(res.toString,res,true))
        case ("-", l:DoubleValue, r:DoubleValue) =>
          val res = l.value - r.value
          _ref = Some(DoubleValue(res.toString,res,true))
        case ("-", l:IntValue, r:DoubleValue) =>
          val res = l.value - r.value
          _ref = Some(DoubleValue(res.toString,res,true))
        case ("-", l:DoubleValue, r:IntValue) =>
          val res = l.value - r.value
          _ref = Some(DoubleValue(res.toString,res,true))
        case ("-", NullValue, _) | ("-", _, NullValue) =>
          _ref = Some(NullValue)

        case ("*", l:IntValue, r:IntValue) =>
          val res = l.value * r.value
          _ref = Some(IntValue(res.toString,res,true))
        case ("*", l:DoubleValue, r:DoubleValue) =>
          val res = l.value * r.value
          _ref = Some(DoubleValue(res.toString,res,true))
        case ("*", l:IntValue, r:DoubleValue) =>
          val res = l.value * r.value
          _ref = Some(DoubleValue(res.toString,res,true))
        case ("*", l:DoubleValue, r:IntValue) =>
          val res = l.value * r.value
          _ref = Some(DoubleValue(res.toString,res,true))
        case ("*", NullValue, _) | ("*", _, NullValue) =>
          _ref = Some(NullValue)

        case ("/", l:IntValue, r:IntValue) =>
          if (r.value == 0) throw new RuntimeException("Zero divide")
          val res = l.value / r.value
          _ref = Some(IntValue(res.toString,res,true))
        case ("/", l:DoubleValue, r:DoubleValue) =>
          if (r.value == 0) throw new RuntimeException("Zero divide")
          val res = l.value / r.value
          _ref = Some(DoubleValue(res.toString,res,true))
        case ("/", l:IntValue, r:DoubleValue) =>
          if (r.value == 0) throw new RuntimeException("Zero divide")
          val res = l.value / r.value
          _ref = Some(DoubleValue(res.toString,res,true))
        case ("/", l:DoubleValue, r:IntValue) =>
          if (r.value == 0) throw new RuntimeException("Zero divide")
          val res = l.value / r.value
          _ref = Some(DoubleValue(res.toString,res,true))
        case ("/", NullValue, _) | ("/", _, NullValue) =>
          _ref = Some(NullValue)

        case ("<<", l:IntValue, r:IntValue) =>
          val res = l.value << r.value
          _ref = Some(IntValue(res.toString,res,true))
        case (">>", l:IntValue, r:IntValue) =>
          val res = l.value >>> r.value
          _ref = Some(IntValue(res.toString,res,true))
        case ("|", l:IntValue, r:IntValue) =>
          val res = l.value | r.value
          _ref = Some(IntValue(res.toString,res,true))
        case ("&", l:IntValue, r:IntValue) =>
          val res = l.value & r.value
          _ref = Some(IntValue(res.toString,res,true))
        case ("^", l:IntValue, r:IntValue) =>
          val res = l.value ^ r.value
          _ref = Some(IntValue(res.toString,res,true))

        case (_,l,r) =>
          throw new RuntimeException(s"Invalid operator between the types:$l $op $r")
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


  override def resolveReference(columns:Map[String,TableColumnValue] = Map.empty[String,TableColumnValue]): Map[String,TableColumnValue] = {
    val lc = if (left != null) left.resolveReference(columns) else columns
    if (right != null) right.resolveReference(lc) else lc
  }
}
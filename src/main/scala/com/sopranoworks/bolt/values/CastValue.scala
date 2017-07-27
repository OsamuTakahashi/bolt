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

import com.google.cloud.{Date, Timestamp}
import com.google.cloud.spanner.Type
import org.joda.time.DateTime

/**
  * 
  * @param expression
  * @param toType
  */
case class CastValue(expression:Value,toType:Type) extends Value with WrappedValue {
  override def text: String = s"CAST(${expression.text} AS $toType)"
  override def eval: Value = {
    if (_ref.isEmpty) {
      toType match {
        case tp if tp == Type.bool() =>
          expression.eval.asValue match {
            case v:BooleanValue => _ref = Some(v)
            case v:IntValue => _ref = Some(BooleanValue(v.value != 0))
            case v:StringValue => _ref = Some(BooleanValue(v.text.toBoolean))
            case v =>
              throw new RuntimeException(s"Can not convert value from ${v.text} to $tp")
          }
        case tp if tp == Type.int64() =>
          expression.eval.asValue match {
            case v:BooleanValue => _ref = Some(IntValue(if (v.f) 1 else 0))
            case v:IntValue => _ref = Some(v)
            case v:DoubleValue => _ref = Some(IntValue(v.value.toLong))
            case v:StringValue => _ref = Some(IntValue(java.lang.Long.parseLong(v.text)))
            case v =>
              throw new RuntimeException(s"Can not convert value from ${v.text} to $tp")
          }

        case tp if tp == Type.float64() =>
          expression.eval.asValue match {
            case v:IntValue => _ref = Some(DoubleValue(v.value.toDouble))
            case v:DoubleValue => _ref = Some(v)
            case v:StringValue => _ref = Some(DoubleValue(v.text.toDouble))
            case v =>
              throw new RuntimeException(s"Can not convert value from ${v.text} to $tp")
          }
        case tp if tp == Type.string() =>
          expression.eval.asValue match {
            case v:BooleanValue => _ref = Some(StringValue(v.text))
            case v:IntValue => _ref = Some(StringValue(v.text))
            case v:DoubleValue => _ref = Some(StringValue(v.text))
            case v:StringValue => _ref = Some(v)
            case v:DateValue => _ref = Some(StringValue(v.text))
            case v:TimestampValue => _ref = Some(StringValue(v.text))
            case v =>
              throw new RuntimeException(s"Can not convert value from ${v.text} to $tp")
          }
        case tp if tp == Type.date() =>
          expression.eval.asValue match {
            case v:DateValue => _ref = Some(v)
            case v:StringValue => _ref = Some(DateValue(Date.parseDate(v.text).toString))
            case v:TimestampValue =>
              val t = new DateTime(Timestamp.parseTimestamp(v.text).getSeconds * 1000)
              _ref = Some(DateValue(Date.fromYearMonthDay(t.getYear,t.getMonthOfYear,t.getDayOfMonth).toString))
            case v =>
              throw new RuntimeException(s"Can not convert value from ${v.text} to $tp")
          }
        case tp if tp == Type.timestamp() =>
          expression.eval.asValue match {
            case v:DateValue =>
              val d = Date.parseDate(v.text)
              _ref = Some(DateValue(Timestamp.of( new DateTime(d.getYear,d.getMonth,d.getDayOfMonth).toDate  ).toString))
            case v:StringValue => _ref = Some(TimestampValue(Timestamp.parseTimestamp(v.text).toString))
            case v:TimestampValue => _ref = Some(v)
            case v =>
              throw new RuntimeException(s"Can not convert value from ${v.text} to $tp")
          }
        case tp if tp == Type.bytes() =>
          expression.eval.asValue match {
            case v:StringValue => _ref = Some(BytesValue(v.text))
            case v:BytesValue => _ref = Some(v)
            case v =>
              throw new RuntimeException(s"Can not convert value from ${v.text} to $tp")
          }
        case _ =>
          throw new RuntimeException(s"Can not convert value to $toType")
      }
    }
    this
  }
  override def resolveReference(columns:Map[String,TableColumnValue]): Map[String,TableColumnValue] = {
    expression.resolveReference(columns)
  }

  override def invalidateEvaluatedValueIfContains(values: List[Value]): Boolean = {
    if (expression.invalidateEvaluatedValueIfContains(values)) {
      _ref = None
      true
    } else false
  }
}
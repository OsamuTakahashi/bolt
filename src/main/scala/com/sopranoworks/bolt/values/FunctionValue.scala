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
import com.sopranoworks.bolt.LikeMatcher
import org.joda.time._

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
  *
  */
case class FunctionValue(name:String,parameters:java.util.List[Value]) extends WrappedValue {

  private def _checkStayUnresolved(f: => Value):Option[Value] = {
    _stayUnresolved = parameters.foldLeft(false) {
      case (ck,v) =>
        ck || v.eval.stayUnresolved
    }
    if (!_stayUnresolved) Some(f) else None
  }


  override def text = s"$name(${parameters.map(_.text).mkString(",")})"
  override def eval: Value = {
    val result = name match {
        //
        //
        //
      case "DATE" =>
        parameters.length match {
          case 3 =>
            _checkStayUnresolved {
              (parameters.get(0).eval.asValue, parameters.get(1).eval.asValue, parameters.get(2).eval.asValue) match {
                case (IntValue(_, y, _), IntValue(_, m, _), IntValue(_, d, _)) =>
                  DateValue(null, new DateTime().withDate(y.toInt, m.toInt, d.toInt))
                case _ =>
                  throw new RuntimeException("Unmatched parameter types for function DATE")
              }
            }
          case 1 =>
            _checkStayUnresolved {
              parameters.get(0).eval.asValue match {
                case t: TimestampValue =>
                  val zone = DateTimeZone.forID("America/Los_Angeles")
                  DateValue(null, new DateTime(new java.util.Date(t.timestamp.getSeconds * 1000 + t.timestamp.getNanos / 1000)).withZone(zone))
                case _ =>
                  throw new RuntimeException("Unmatched parameter types for function DATE")
              }
            }
          case 2 =>
            _checkStayUnresolved {
              (parameters.get(0).eval.asValue, parameters.get(1).eval.asValue) match {
                case (t: TimestampValue, z: StringValue) =>
                  val zone = DateTimeZone.forID(z.text)
                  DateValue(null, new DateTime(new java.util.Date(t.timestamp.getSeconds * 1000 + t.timestamp.getNanos / 1000)).withZone(zone))
                case _ =>
                  throw new RuntimeException("Unmatched parameter types for function DATE")
              }
            }
          case _ =>
            throw new RuntimeException("Unmatched parameter types for function DATE")
        }


      case "DATE_ADD" =>
        if (parameters.length != 2)
          throw new RuntimeException("Function DATE_ADD takes 2 parameters")
        _checkStayUnresolved {
          (parameters.get(0).eval.asValue.castTo(Type.date()), parameters.get(1).eval.asValue) match {
            case (dv: DateValue, iv: IntervalValue) =>
              val nd = iv match {
                case IntervalValue(d, "DAY") =>
                  dv.date.plusDays(d)
                case IntervalValue(d, "WEEK") =>
                  dv.date.plusWeeks(d)
                case IntervalValue(d, "MONTH") =>
                  dv.date.plusMonths(d)
                case IntervalValue(d, "QUARTER") =>
                  dv.date.plusMonths(d * 3)
                case IntervalValue(d, "YEAR") =>
                  dv.date.plusYears(d)
                case _ =>
                  throw new RuntimeException(s"Unrecognized date part signature ${iv.unit}")
              }
              DateValue(null,nd)

            case (_, b) =>
              throw new RuntimeException("The second parameter of DATE_ADD must be INTERVAL")
          }
        }

        
      case "DATE_SUB" =>
        if (parameters.length != 2)
          throw new RuntimeException("Function DATE_SUB takes 2 parameters")
        _checkStayUnresolved {
          (parameters.get(0).eval.asValue.castTo(Type.date()), parameters.get(1).eval.asValue) match {
            case (dv: DateValue, iv: IntervalValue) =>
              val nd = iv match {
                case IntervalValue(d, "DAY") =>
                  dv.date.minusDays(d)
                case IntervalValue(d, "WEEK") =>
                  dv.date.minusWeeks(d)
                case IntervalValue(d, "MONTH") =>
                  dv.date.minusMonths(d)
                case IntervalValue(d, "QUARTER") =>
                  dv.date.minusMonths(d * 3)
                case IntervalValue(d, "YEAR") =>
                  dv.date.minusYears(d)
                case _ =>
                  throw new RuntimeException(s"Unrecognized date part signature ${iv.unit}")
              }
              DateValue(null,nd)
              
            case (_, b) =>
              throw new RuntimeException("The second parameter of DATE_SUB must be INTERVAL")
          }
        }

      case "DATE_DIFF" =>
        if (parameters.length != 3)
          throw new RuntimeException("Function DATE_DIFF takes 3 parameters")

        _checkStayUnresolved {
          (parameters.get(0).eval.asValue.castTo(Type.date()), parameters.get(1).eval.asValue.castTo(Type.date()), parameters.get(2)) match {
            case (a: DateValue, b: DateValue, IdentifierValue("DAY", _)) =>
              val d = new Duration(b.date, a.date)
              IntValue(d.getStandardDays)

            case (a: DateValue, b: DateValue, IdentifierValue("WEEK", _)) =>
              val d = Weeks.weeksBetween(b.date, a.date)
              IntValue(d.getWeeks)

            case (a: DateValue, b: DateValue, IdentifierValue("MONTH", _)) =>
              val d = Months.monthsBetween(b.date, a.date)
              IntValue(d.getMonths)

            case (a: DateValue, b: DateValue, IdentifierValue("QUATER", _)) =>
              val d = Months.monthsBetween(b.date, a.date)
              IntValue(d.getMonths / 3)

            case (a: DateValue, b: DateValue, IdentifierValue("YEAR", _)) =>
              val d = Years.yearsBetween(b.date, a.date)
              IntValue(d.getYears)

            case (a: DateValue, b: DateValue, IdentifierValue(f, _)) =>
              throw new RuntimeException(s"Unrecognized date part signature $f")

            case (_, _, e) =>
              throw new RuntimeException(s"Invalid date part type '${e.text}'")
          }
        }

        
      case "DATE_TRUNC" =>
        if (parameters.length != 2)
          throw new RuntimeException("Function DATE_TRUNC takes 2 parameters")

        _checkStayUnresolved {
          (parameters.get(0).eval.asValue.castTo(Type.date()), parameters.get(1)) match {
            case (d: DateValue, IdentifierValue("DAY", _)) =>
              d

            case (d: DateValue, IdentifierValue("WEEK", _)) =>
              DateValue(null, d.date.minusDays((d.date.getDayOfWeek + 1) % 8 - 1))

            case (d: DateValue, IdentifierValue("MONTH", _)) =>
              DateValue(null, d.date.minusDays(d.date.getDayOfMonth - 1))

            case (d: DateValue, IdentifierValue("QUATER", _)) =>
              val m = d.date.getMonthOfYear
              DateValue(null, new DateTime(d.date.getYear, d.date.getMonthOfYear - ((m - 1) % 3), 1))

            case (d: DateValue, IdentifierValue("YEAR", _)) =>
              DateValue(null, new DateTime(d.date.getYear, 1, 1))

            case (_: DateValue, IdentifierValue(f, _)) =>
              throw new RuntimeException(s"Unrecognized date part signature $f")

            case (_, e) =>
              throw new RuntimeException(s"Invalid date part type '${e.text}'")
          }
        }

        //
        //
        //

      case "NOW" =>
        if (parameters.nonEmpty)
          throw new RuntimeException("Function NOW takes no parameter")
          val ts = Timestamp.now()
          Some(TimestampValue(ts.toString))

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

        // Array functions
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
        
        // internal functions
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

      case _ =>
        throw new RuntimeException(s"Unevaluatable function $name")
    }
    _ref = result
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
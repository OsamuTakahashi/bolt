/**
  * Bolt
  * DateFunctions
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt.values.functions

import com.google.cloud.spanner.Type
import com.sopranoworks.bolt.values._
import org.joda.time._

import scala.collection.JavaConversions._

trait DateFunctions { _ : FunctionValue =>
  val dateFunctions:PartialFunction[String,Option[Value]] = {
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
                  DateValue(null, new DateTime().withZone(zone).withMillis(t.timestamp._1.getMillis))
              case _ =>
                throw new RuntimeException("Unmatched parameter types for function DATE")
            }
          }
        case 2 =>
          _checkStayUnresolved {
            (parameters.get(0).eval.asValue, parameters.get(1).eval.asValue) match {
              case (t: TimestampValue, z: StringValue) =>
                val zone = DateTimeZone.forID(z.text)
                DateValue(null, new DateTime().withZone(zone).withMillis(t.timestamp._1.getMillis))
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
                dv.datetime.plusDays(d)
              case IntervalValue(d, "WEEK") =>
                dv.datetime.plusWeeks(d)
              case IntervalValue(d, "MONTH") =>
                dv.datetime.plusMonths(d)
              case IntervalValue(d, "QUARTER") =>
                dv.datetime.plusMonths(d * 3)
              case IntervalValue(d, "YEAR") =>
                dv.datetime.plusYears(d)
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
                dv.datetime.minusDays(d)
              case IntervalValue(d, "WEEK") =>
                dv.datetime.minusWeeks(d)
              case IntervalValue(d, "MONTH") =>
                dv.datetime.minusMonths(d)
              case IntervalValue(d, "QUARTER") =>
                dv.datetime.minusMonths(d * 3)
              case IntervalValue(d, "YEAR") =>
                dv.datetime.minusYears(d)
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
            val d = new Duration(b.datetime, a.datetime)
            IntValue(d.getStandardDays)

          case (a: DateValue, b: DateValue, IdentifierValue("WEEK", _)) =>
            val d = Weeks.weeksBetween(b.datetime, a.datetime)
            IntValue(d.getWeeks)

          case (a: DateValue, b: DateValue, IdentifierValue("MONTH", _)) =>
            val d = Months.monthsBetween(b.datetime, a.datetime)
            IntValue(d.getMonths)

          case (a: DateValue, b: DateValue, IdentifierValue("QUATER", _)) =>
            val d = Months.monthsBetween(b.datetime, a.datetime)
            IntValue(d.getMonths / 3)

          case (a: DateValue, b: DateValue, IdentifierValue("YEAR", _)) =>
            val d = Years.yearsBetween(b.datetime, a.datetime)
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
            DateValue(null, d.datetime.minusDays((d.datetime.getDayOfWeek + 1) % 8 - 1))

          case (d: DateValue, IdentifierValue("MONTH", _)) =>
            DateValue(null, d.datetime.minusDays(d.datetime.getDayOfMonth - 1))

          case (d: DateValue, IdentifierValue("QUATER", _)) =>
            val m = d.datetime.getMonthOfYear
            DateValue(null, new DateTime(d.datetime.getYear, d.datetime.getMonthOfYear - ((m - 1) % 3), 1))

          case (d: DateValue, IdentifierValue("YEAR", _)) =>
            DateValue(null, new DateTime(d.datetime.getYear, 1, 1))

          case (_: DateValue, IdentifierValue(f, _)) =>
            throw new RuntimeException(s"Unrecognized date part signature $f")

          case (_, e) =>
            throw new RuntimeException(s"Invalid date part type '${e.text}'")
        }
      }
  }
}

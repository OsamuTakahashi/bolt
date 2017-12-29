/**
  * Bolt
  * TimestampFunctions
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt.values.functions

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Type
import com.sopranoworks.bolt.values._

import scala.collection.JavaConversions._

trait TimestampFunctions { _ : FunctionValue =>
  val timestampFunctions : PartialFunction[String,Option[Value]] = {
    case "NOW" =>
      if (parameters.nonEmpty)
        throw new RuntimeException("Function NOW takes no parameter")
      val ts = Timestamp.now()
      Some(TimestampValue(ts.toString))
    case "TIMESTAMP_ADD" =>
      if (parameters.length != 2)
        throw new RuntimeException("Function DATE_ADD takes 2 parameters")
      _checkStayUnresolved {
        (parameters.get(0).eval.asValue.castTo(Type.timestamp()), parameters.get(1).eval.asValue) match {
          case (tv: TimestampValue, iv: IntervalValue) =>
            val (nd,ns) = iv match {
              case IntervalValue(d, "NANOSECOND") =>
                val (dd,nn) = tv.timestamp
                val nnn = nn + d
                (dd.plusMillis(nnn / 1000000),nnn % 1000000)
              case IntervalValue(d, "MICROSECOND") =>
                val (dd,nn) = tv.timestamp
                val nnn = nn + d * 1000
                (dd.plusMillis(nnn / 1000000),nnn % 1000000)
              case IntervalValue(d, "MILLISECOND") =>
                (tv.datetime.plusMillis(d),tv.nano)
              case IntervalValue(d, "SECOND") =>
                (tv.datetime.plusSeconds(d),tv.nano)
              case IntervalValue(d, "MINUTE") =>
                (tv.datetime.plusMinutes(d),tv.nano)
              case IntervalValue(d, "HOUR") =>
                (tv.datetime.plusHours(d),tv.nano)

              case IntervalValue(d, "DAY") =>
                (tv.datetime.plusDays(d),tv.nano)
              case IntervalValue(d, "WEEK") =>
                (tv.datetime.plusWeeks(d),tv.nano)
              case IntervalValue(d, "MONTH") =>
                (tv.datetime.plusMonths(d),tv.nano)
              case IntervalValue(d, "QUARTER") =>
                (tv.datetime.plusMonths(d * 3),tv.nano)
              case IntervalValue(d, "YEAR") =>
                (tv.datetime.plusYears(d),tv.nano)
              case _ =>
                throw new RuntimeException(s"Unrecognized date part signature ${iv.unit}")
            }

            TimestampValue(null,nd,ns)

          case (_, b) =>
            throw new RuntimeException("The second parameter of TIMESTAMP_ADD must be INTERVAL")
        }
      }

  }
}

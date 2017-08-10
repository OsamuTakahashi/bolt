package com.sopranoworks.bolt

import org.joda.time.DateTime
import org.specs2.mutable.Specification

class TimestampParserTest extends Specification {

  "parse timestamp" should {
    "normally success" in{
      val (tm,n) = TimestampParser("2014-09-27 12:30:00.45 America/Los_Angeles")

      tm.getYear must_== 2014
      tm.getMonthOfYear must_== 9
      tm.getDayOfMonth  must_== 27

      tm.getHourOfDay     must_== 12
      tm.getMinuteOfHour   must_== 30
      tm.getSecondOfMinute must_== 0
      tm.getMillisOfSecond must_== 450
    }
    "with zone suffix" in {
      val (tm,n) = TimestampParser("2014-09-27 12:30:00.45-08")

      tm.getYear must_== 2014
      tm.getMonthOfYear must_== 9
      tm.getDayOfMonth  must_== 27

      tm.getHourOfDay     must_== 12
      tm.getMinuteOfHour   must_== 30
      tm.getSecondOfMinute must_== 0
      tm.getMillisOfSecond must_== 450
    }
    "with ISO format" in {
      val (tm,n) = TimestampParser("2014-09-27T12:30:00.45-08")

      tm.getYear must_== 2014
      tm.getMonthOfYear must_== 9
      tm.getDayOfMonth  must_== 27

      tm.getHourOfDay     must_== 12
      tm.getMinuteOfHour   must_== 30
      tm.getSecondOfMinute must_== 0
      tm.getMillisOfSecond must_== 450
    }
    "with ISO format2" in {
      val (tm,n) = TimestampParser("2014-09-27T12:30:00.45Z")

      tm.getYear must_== 2014
      tm.getMonthOfYear must_== 9
      tm.getDayOfMonth  must_== 27

      tm.getHourOfDay     must_== 12
      tm.getMinuteOfHour   must_== 30
      tm.getSecondOfMinute must_== 0
      tm.getMillisOfSecond must_== 450
    }
    "only Date" in {
      val (tm,n) = TimestampParser("2014-09-27")

      tm.getYear must_== 2014
      tm.getMonthOfYear must_== 9
      tm.getDayOfMonth  must_== 27
    }
    "date and timezone" in {
      val (tm,n) = TimestampParser("2014-09-27 America/Los_Angeles")

      tm.getYear must_== 2014
      tm.getMonthOfYear must_== 9
      tm.getDayOfMonth  must_== 27
    }
    "hour only" in{
      val (tm,n) = TimestampParser("2014-09-27 12")

      tm.getYear must_== 2014
      tm.getMonthOfYear must_== 9
      tm.getDayOfMonth  must_== 27

      tm.getHourOfDay     must_== 12
    }
    "hour and minute" in{
      val (tm,n) = TimestampParser("2014-09-27 12:30")

      tm.getYear must_== 2014
      tm.getMonthOfYear must_== 9
      tm.getDayOfMonth  must_== 27

      tm.getHourOfDay     must_== 12
      tm.getMinuteOfHour   must_== 30
    }
  }
}

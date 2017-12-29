package com.sopranoworks.bolt.values

import com.google.cloud.{Date, Timestamp}
import com.google.cloud.spanner.{Mutation, Type}
import com.sopranoworks.bolt.TimestampParser
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

trait DateTimeValue extends Value with TextSetter with LiteralValue {
  def datetime:DateTime

  override def getField(fieldName: String): Value = {
    this.eval
    fieldName match {
      case "DAYOFWEEK" =>
        IntValue((datetime.getDayOfWeek + 1) % 8)
      case "DAY" =>
        IntValue(datetime.getDayOfMonth)
      case "DAYOFYEAR" =>
        IntValue(datetime.getDayOfYear)
      case "MONTH" =>
        IntValue(datetime.getMonthOfYear)
      case "QUARTER" =>
        IntValue((datetime.getMonthOfYear - 1) / 3 + 1)
      case "YEAR" =>
        IntValue(datetime.getYear)
      case _ =>
        throw new RuntimeException(s"The value '$text' does not have a field #$fieldName.")
    }
  }
}

case class TimestampValue(var timestampText:String,private var _datetime :DateTime = null,private var _nano:Int = 0) extends DateTimeValue {

  private def _timestampFormat(dt:DateTime):String =
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").print(dt) + ".%09d".format(dt.getMillisOfSecond * 1000000 + _nano) +
      DateTimeFormat.forPattern("ZZ").print(dt)

  override def text: String = {
    if (timestampText == null) {
      Option(_datetime) match {
        case Some(dt) =>
          timestampText = _timestampFormat(dt)
        case None =>
          throw new RuntimeException("Uninitialized Timestamp value used")
      }
    }
    timestampText
  }
  override def qtext:String = s"TIMESTAMP '$text'"
  override def spannerType: Type = Type.timestamp()
  override def isEqualValue(v:Value):Boolean =
    v.isInstanceOf[TimestampValue] && v.asInstanceOf[TimestampValue].text == text

  override def eval: Value = {
    if (_datetime == null) {
      val (t,n) = TimestampParser(text)
      _datetime = t
      _nano = n
    }
    this
  }

  override def setTo(m: Mutation.WriteBuilder, key: String): Unit = {
    if (_datetime != null) {
      val d = _datetime.getMillis
      m.set(key).to(Timestamp.ofTimeSecondsAndNanos(d / 1000,((d % 1000) * 1000000 + _nano).toInt))
    } else {
      m.set(key).to(text)
    }
  }

  override def getField(fieldName: String): Value = {
    this.eval
    fieldName match {
      case "MILLISECOND" =>
        IntValue(datetime.getMillisOfSecond)
      case "MICROSECOND" =>
        IntValue(_nano / 1000)
      case "NANOSECOND" =>
        IntValue(_nano % 1000)
        
      case _ =>
        super.getField(fieldName)
    }
  }

  def timestamp = (_datetime,_nano)
  def datetime = _datetime
  def nano = _nano
}

case class DateValue(textDate:String,private var dateTime:DateTime = null) extends DateTimeValue {
  override def text: String = {
    if (textDate == null) {
      Date.fromYearMonthDay(dateTime.getYear,dateTime.getMonthOfYear,dateTime.getDayOfMonth).toString
    } else textDate
  }

  override def qtext:String = s"DATE '$text'"
  override def spannerType: Type = Type.date()
  override def isEqualValue(v:Value):Boolean =
    v.isInstanceOf[DateValue] && v.asInstanceOf[DateValue].text == text

  override def eval: Value = {
    if (dateTime == null) {
      val d = Date.parseDate(text)
      dateTime = new DateTime().withDate(d.getYear, d.getMonth, d.getDayOfMonth)
    }
    this
  }

  def datetime:DateTime = {
    this.eval
    dateTime
  }
}

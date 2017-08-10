/**
  * Bolt
  * TimestampParser
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt

import org.joda.time.{DateTime, DateTimeZone}

import scala.util.parsing.combinator._

/**
  * The timestamp parser for both spanner type timestamp format and ISO 8601 format
  */
object TimestampParser extends RegexParsers {
  case class Date(y:Int,m:Int,d:Int)
  case class Time(h:Int,m:Int,s:Int,n:Int,z:Option[ZoneSfx])
  case class ZoneSfx(h:Int,m:Int)

  def date = """\d{4}""".r ~ "-" ~ """\d{1,2}""".r ~ "-" ~ """\d{1,2}""".r ^^ { case (y ~ "-" ~ m ~ "-" ~ d) => Date(y.toInt,m.toInt,d.toInt) }
  def time = """T?""".r ~ """\d{1,2}""".r ~  min.? ~ zone_sfx.? ^^ {
    case (_ ~ h ~ m ~ z) =>
      m match {
        case Some(mm) =>
          Time(h.toInt,mm._1,mm._2._1,mm._2._2,z)
        case None =>
          Time(h.toInt,0,0,0,z)
      }
  }

  def min = ":" ~ """\d{1,2}""".r ~ sec.? ^^ { case (_ ~ m ~ s) => (m.toInt,s.getOrElse((0,0)))}
  def sec = ":" ~ """\d{1,2}""".r ~ nano.? ^^ { case (_ ~ s ~ n ) => (s.toInt,n.getOrElse(0))}

  def nano = """(\.|,)""".r  ~ """\d{1,9}""".r ^^ { case (_ ~ n) => (if (n.length < 9) n + "0" * (9 - n.length) else n).toInt }
  def zone_sfx = ("""(-|\+)\d{1,2}""".r ~ (":" ~ """\d{1,2}""".r).? ^^ {
    case (h ~ Some(":" ~ m)) => ZoneSfx(h.toInt,m.toInt)
    case (h ~ None) => ZoneSfx(h.toInt,0)
  } | "Z" ^^ { case _ => ZoneSfx(0,0) })

  def zone = """[a-zA-Z_]+/[a-zA-Z_]+""".r

  def timestamp = date ~ time.? ~ zone.? ^^ {
    case (d ~ t ~ z) =>
      val dt = new DateTime()
      val dz = z match {
        case Some(zn) =>
          dt.withZone(DateTimeZone.forID(zn))
        case None =>
          dt
      }
      val dzz = (t match {
        case Some(Time(_,_,_,_,Some(zsfx))) =>
          dt.withZone(DateTimeZone.forOffsetHoursMinutes(zsfx.h,zsfx.m))
        case _ =>
          dz
      }).withDate(d.y,d.m,d.d)

      t match {
        case Some(tm) =>
          val dd = dzz.withTime(tm.h,tm.m,tm.s,tm.n / 1000000)
          (dd,tm.n % 1000000)
        case None =>
          (dzz,0)
      }
  }

  def apply(str:String):(DateTime,Int) = {
    parseAll(timestamp,str) match {
      case Success(r,_) =>
        r
      case Failure(err,next) =>
        throw new RuntimeException(s"Timestamp Parse Error: $err on ${next.pos.line}:${next.pos.column}")
      case Error(err,next) =>
        throw new RuntimeException(s"Timestamp Parse Error: $err on ${next.pos.line}:${next.pos.column}")
    }
  }
}

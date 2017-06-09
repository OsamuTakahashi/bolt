package com.sopranoworks.bolt

import scala.collection.JavaConverters._

/**
  * Created by takahashi on 2017/03/29.
  */
trait Value {
  def text:String
  def qtext:String = text
}

case class StringValue(text:String) extends Value {
  override def qtext:String = s"'$text'"
}

case class IntValue(text:String) extends Value

case object NullValue extends Value {
  override def text: String = "NULL"
}

case class ArrayValue(values:java.util.List[String]) extends Value  {
  override def text:String = s"(${values.asScala.mkString(",")})"
}
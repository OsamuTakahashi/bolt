package com.sopranoworks.bolt

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
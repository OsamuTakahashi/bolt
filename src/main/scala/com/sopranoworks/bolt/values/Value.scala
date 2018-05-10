/**
  * Bolt
  * Value
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt.values

import com.google.cloud.spanner.{Mutation, Type}

/**
  * Created by takahashi on 2017/03/29.
  */
trait Value {
  def text:String
  def qtext:String = text

  def stayUnresolved:Boolean = false

  def eval:Value = this
  def evaluatable:Boolean = true

  def invalidateEvaluatedValueIfContains(values:List[Value]):Boolean = false

  def resolveReference(columns:Map[String,TableColumnValue] = Map.empty[String,TableColumnValue]):Map[String,TableColumnValue] = columns

  def apply():Value = this
  def asValue:Value = this

  def isBoolean:Boolean = false
  def spannerType:Type = null

  def asArray:ArrayValue = throw new RuntimeException("Could not treat the value as Array")

  def setTo(m:Mutation.WriteBuilder,key:String):Unit = {
    throw new RuntimeException(s"The value is not storable:'${text}'")
  }

  def isEqualValue(v:Value):Boolean = false

  def getField(fieldIdx:Int):Value = throw new RuntimeException(s"The value '$text' does not have a field #$fieldIdx.")
  def getField(fieldName:String):Value = throw new RuntimeException(s"The value '$text' does not have a field '$fieldName'.")

  def castTo(toType:Type):Value =
    CastValue.cast(this,toType).getOrElse {
      throw new RuntimeException(s"Can not convert value from ${this.text} to $toType")
    }
}

/**
  *
  */
trait WrappedValue extends Value {
  protected var _stayUnresolved = false
  override def stayUnresolved:Boolean = _ref.map(_.stayUnresolved).getOrElse(_stayUnresolved)

  def setStayUnresolved(f:Boolean):Unit = _stayUnresolved = true

  protected var _ref:Option[Value] = None

  override def asValue: Value = _ref.map(_.eval.asValue).getOrElse(this)
  override def asArray: ArrayValue = _ref.map(_.eval.asArray).getOrElse(super.asArray)

  override def spannerType: Type = _ref.map(_.spannerType).orNull

  override def eval: Value = _ref.map(_.eval).getOrElse(this)
  override def evaluatable: Boolean = _ref.isDefined

  override def invalidateEvaluatedValueIfContains(values:List[Value]):Boolean = {
    _ref match {
      case Some(v) => v.invalidateEvaluatedValueIfContains(values)
      case None => false
    }
  }

  override def isEqualValue(v:Value):Boolean = _ref.exists(_.isEqualValue(v))

  override def apply():Value = _ref.map(_.apply()).getOrElse(this)
  
  override def setTo(m: Mutation.WriteBuilder, key: String): Unit =
    _ref match {
      case Some(v) =>
        v.eval.asValue.setTo(m,key)
      case None =>
        super.setTo(m,key)
    }

  override def resolveReference(columns: Map[String, TableColumnValue]): Map[String, TableColumnValue] = _ref.map(_.resolveReference(columns)).getOrElse(columns)
  override def getField(fieldIdx: Int): Value = _ref.map(_.getField(fieldIdx)).getOrElse(super.getField(fieldIdx))
  override def getField(fieldName: String): Value = _ref.map(_.getField(fieldName)).getOrElse(super.getField(fieldName))
}

trait TextSetter { _ : Value =>
  override def setTo(m:Mutation.WriteBuilder,key:String):Unit = {
    m.set(key).to(this.eval.asValue.text)
  }
}

trait LiteralValue

case object NullValue extends Value {
  override def text: String = "NULL"
  override def setTo(m: Mutation.WriteBuilder, key: String): Unit = {
    m.set(key).to(null:java.lang.String)
  }
  override def isEqualValue(v:Value):Boolean = v == NullValue
}

case class StringValue(text:String) extends Value with TextSetter with LiteralValue {
  override def qtext:String = s"'$text'"
  override def spannerType: Type = Type.string()
  override def isEqualValue(v:Value):Boolean =
    v.isInstanceOf[StringValue] && v.asInstanceOf[StringValue].text == text
}

case class BooleanValue(f:Boolean) extends Value with TextSetter with LiteralValue {
  override def text:String = if (f) "TRUE" else "FALSE"
  override def isBoolean:Boolean = true
  override def spannerType: Type = Type.bool()
  override def isEqualValue(v:Value):Boolean =
    v.isInstanceOf[BooleanValue] && v.asInstanceOf[BooleanValue].f == f

  override def setTo(m: Mutation.WriteBuilder, key: String): Unit =
    m.set(key).to(f)
}

case class IntValue(text:String,var value:Long = 0,var evaluated:Boolean = false) extends Value with TextSetter with LiteralValue {
  def this(i:Long) = this(i.toString,i,true)
  override def eval: Value = {
    if (!evaluated) {
      value = java.lang.Long.parseLong(text)
      evaluated = true
    }
    this
  }
  override def spannerType: Type = Type.int64()
  override def isEqualValue(v:Value):Boolean =
    v.isInstanceOf[IntValue] && v.asInstanceOf[IntValue].value == value
}

object IntValue {
  def apply(i:Long):IntValue = new IntValue(i)
}

case class DoubleValue(text:String,var value:Double = 0,var evaluated:Boolean = false) extends Value with TextSetter with LiteralValue {
  def this(v:Double) = this(v.toString,v,true)
  override def eval: Value = {
    if (!evaluated) {
      value = java.lang.Double.parseDouble(text)
      evaluated = true
    }
    this
  }
  override def spannerType: Type = Type.float64()
  override def isEqualValue(v:Value):Boolean =
    v.isInstanceOf[DoubleValue] && v.asInstanceOf[DoubleValue].value == value
}

object DoubleValue {
  def apply(v:Double):DoubleValue = new DoubleValue(v)
}

case class BytesValue(text:String) extends Value with LiteralValue {
  override def spannerType: Type = Type.bytes()
}
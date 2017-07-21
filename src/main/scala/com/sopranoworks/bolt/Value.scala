/**
  * Bolt
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt

import com.google.cloud.Timestamp
import com.google.cloud.spanner.{Struct, Type}
import com.sopranoworks.bolt.Bolt.Nat

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
  * Created by takahashi on 2017/03/29.
  */
trait Value {
  def text:String
  def qtext:String = text
  def eval:Value = this
  def asValue:Value = this
  def isExpression:Boolean = false
  def isBoolean:Boolean = false
  def spannerType:Type = null

  def asArray:ArrayValue = throw new RuntimeException("Could not treat the value as Array")
}

case object NullValue extends Value {
  override def text: String = "NULL"
}

case class StringValue(text:String) extends Value {
  override def qtext:String = s"'$text'"
  override def spannerType: Type = Type.string()
}

case class BooleanValue(f:Boolean) extends Value {
  override def text:String = if (f) "TRUE" else "FALSE"
  override def isBoolean:Boolean = true
  override def spannerType: Type = Type.bool()
}

case class IntValue(text:String,var value:Long = 0,var evaluated:Boolean = false) extends Value {
  override def eval: Value = {
    if (!evaluated) {
      value = java.lang.Long.parseLong(text)
      evaluated = true
    }
    this
  }
  override def spannerType: Type = Type.int64()
}

case class DoubleValue(text:String,var value:Double = 0,var evaluated:Boolean = false) extends Value {
  override def eval: Value = {
    if (!evaluated) {
      value = java.lang.Double.parseDouble(text)
      evaluated = true
    }
    this
  }
  override def spannerType: Type = Type.float64()
}

case class TimestampValue(text:String) extends Value {
  override def qtext:String = s"'$text'"
  override def spannerType: Type = Type.timestamp()
}

case class DateValue(text:String) extends Value {
  override def qtext:String = s"'$text'"
  override def spannerType: Type = Type.date()
}

case class ArrayValue(var values:java.util.List[Value],var evaluated:Boolean = false,var arrayType:Type = null) extends Value  {
  private def _isValidArray:Boolean = {
    if (values.isEmpty) {
      true
    } else {
      val t = if (arrayType != null) arrayType else values.get(0).spannerType
      if (t != null) {
        arrayType = t
        values.forall(_.spannerType == t)
      } else {
        false
      }
    }
  }

  override def eval: Value = {
    if (!evaluated) {
      values = values.map(_.eval.asValue).filter(_ != NullValue)
      if (!_isValidArray)
        throw new RuntimeException("")
      evaluated = true
    }
    this
  }

  def length:Int = values.length

  override def text:String = values.map(_.text).mkString("[",",","]")
  override def spannerType: Type = arrayType

  override def asArray: ArrayValue = this

  def offset(v:Value):Value = {
    val n = v.eval.asValue match {
      case NullValue =>
        0
      case IntValue(_,i,_) =>
        i.toInt
      case _ =>
        throw new RuntimeException("Array offset type must be int64")
    }
    if (n < 0 || values.length <= n) throw new ArrayIndexOutOfBoundsException
    values.get(n)
  }

  def ordinal(v:Value):Value = {
    val n = v.eval.asValue match {
      case NullValue =>
        0
      case IntValue(_,i,_) =>
        i.toInt - 1
      case _ =>
        throw new RuntimeException("Array offset type must be int64")
    }
    if (n < 0 || values.length <= n) throw new ArrayIndexOutOfBoundsException
    values.get(n)
  }
}

case class FunctionValue(name:String,parameters:java.util.List[Value]) extends Value {
  private var _result:Option[Value] = None
  override def spannerType: Type = _result.map(_.spannerType).orNull

  override def text = s"$name(${parameters.map(_.text).mkString(",")})"
  override def eval: Value = {
    name match {
      case "NOW" =>
        if (parameters.nonEmpty)
          throw new RuntimeException("Function NOW takes no parameter")
        val ts = Timestamp.now()
        _result = Some(TimestampValue(ts.toString))

      case "COALESCE" =>
        if (parameters.isEmpty)
          throw new RuntimeException("Function COALESCE takes least 1 parameter")
        _result = parameters.find(_.eval.asValue != NullValue).orElse(Some(NullValue))
      case "IF" =>
        if (parameters.length != 3)
          throw new RuntimeException("Function IF takes 3 parameters")
        val cond = parameters.get(0).eval.asValue
        cond match {
          case BooleanValue(f) =>
            _result = Some((if (f) parameters.get(1) else parameters(2)).eval.asValue)
          case _ =>
            if (!cond.isBoolean)
              throw new RuntimeException("The first parameter of function IF must be boolean expression")
        }
      case "IFNULL" =>
        if (parameters.length != 2)
          throw new RuntimeException("Function IF takes 2 parameters")
        parameters.get(0).eval.asValue match {
          case NullValue =>
            _result = Some(parameters.get(1))
          case v =>
            _result = Some(v)
        }
        
        // Array functions
      case "ARRAY_LENGTH" =>
        if (parameters.length != 1)
          throw new RuntimeException("Function ARRAY_LENGTH takes 1 parameter")

        parameters.get(0).eval.asValue match {
          case arr:ArrayValue =>
            val l = arr.length
            _result = Some(IntValue(l.toString,l,true))
          case _ =>
            throw new RuntimeException("The parameter type of ARRAY_LENGTH must be ARRAY")
        }


        // internal functions
      case "$ARRAY" =>
        if (parameters.length != 1)
          throw new RuntimeException("Function IF takes 1 parameter")

        _result = Some(parameters.head match {
          case v:ArrayValue =>
            v
          case q:SubqueryValue =>
            q.eval.asArray
          case _ =>
            throw new RuntimeException("Could not convert to an array")
        })

      case "$OFFSET" =>
        if (parameters.length != 2)
          throw new RuntimeException("Function IF takes 1 parameter")
        (parameters.get(0).eval.asValue,parameters.get(1).eval.asValue) match {
          case (arr:ArrayValue,i:IntValue) =>
            _result = Some(arr.offset(i))
          case (_,_:IntValue) =>
            throw new RuntimeException("Array element access with non-array value")
          case (_:ArrayValue,_:IntValue) =>
            throw new RuntimeException("Index type of array element access must be INT64")
        }
        
      case "$ORDINAL" =>
        if (parameters.length != 2)
          throw new RuntimeException("Function IF takes 1 parameter")
        (parameters.get(0).eval.asValue,parameters.get(1).eval.asValue) match {
          case (arr:ArrayValue,i:IntValue) =>
            _result = Some(arr.ordinal(i))
          case (_,_:IntValue) =>
            throw new RuntimeException("Array element access with non-array value")
          case (_:ArrayValue,_:IntValue) =>
            throw new RuntimeException("Index type of array element access must be INT64")
        }

      case _ =>
        throw new RuntimeException(s"Unevaluatable function $name")
    }
    this
  }
  override def asValue: Value =
    _result.getOrElse(this.eval.asValue)
}

case class StructValue() extends Value {
  private var members:List[Value] = Nil
  override def text = s"STRUCT(${members.map(_.text).mkString(",")})"
  override def eval: Value = throw new RuntimeException("Struct is not supported yet")

  def addValue(v:Value):Unit = {
    members :+= v
  }
}

case class ExpressionValue(op:String,left:Value,right:Value) extends Value {
  private var _result:Option[Value] = None

  override def text = s"${left.text} $op ${right.text}"
  override def isExpression: Boolean = true
  override def eval: Value = {
    if (_result.isEmpty) {
      (op, left.eval.asValue, if (right != null) right.eval.asValue else right) match {
        case ("-", l:IntValue, null) =>
          val v = -l.value
          _result = Some(IntValue(v.toString,v,true))
        case ("-", l:DoubleValue, null) =>
          val v = -l.value
          _result = Some(DoubleValue(v.toString,v,true))
        case ("~", l:IntValue, null) =>
          val v = ~l.value
          _result = Some(IntValue(v.toString,v,true))

        case ("+", l:IntValue, r:IntValue) =>
          val res = l.value + r.value
          _result = Some(IntValue(res.toString,res,true))
        case ("+", l:DoubleValue, r:DoubleValue) =>
          val res = l.value + r.value
          _result = Some(DoubleValue(res.toString,res,true))
        case ("+", l:IntValue, r:DoubleValue) =>
          val res = l.value + r.value
          _result = Some(DoubleValue(res.toString,res,true))
        case ("+", l:DoubleValue, r:IntValue) =>
          val res = l.value + r.value
          _result = Some(DoubleValue(res.toString,res,true))
        case ("+", NullValue, _) | ("+", _, NullValue) =>
          _result = Some(NullValue)

        case ("-", l:IntValue, r:IntValue) =>
          val res = l.value - r.value
          _result = Some(IntValue(res.toString,res,true))
        case ("-", l:DoubleValue, r:DoubleValue) =>
          val res = l.value - r.value
          _result = Some(DoubleValue(res.toString,res,true))
        case ("-", l:IntValue, r:DoubleValue) =>
          val res = l.value - r.value
          _result = Some(DoubleValue(res.toString,res,true))
        case ("-", l:DoubleValue, r:IntValue) =>
          val res = l.value - r.value
          _result = Some(DoubleValue(res.toString,res,true))
        case ("-", NullValue, _) | ("-", _, NullValue) =>
          _result = Some(NullValue)

        case ("*", l:IntValue, r:IntValue) =>
          val res = l.value * r.value
          _result = Some(IntValue(res.toString,res,true))
        case ("*", l:DoubleValue, r:DoubleValue) =>
          val res = l.value * r.value
          _result = Some(DoubleValue(res.toString,res,true))
        case ("*", l:IntValue, r:DoubleValue) =>
          val res = l.value * r.value
          _result = Some(DoubleValue(res.toString,res,true))
        case ("*", l:DoubleValue, r:IntValue) =>
          val res = l.value * r.value
          _result = Some(DoubleValue(res.toString,res,true))
        case ("*", NullValue, _) | ("*", _, NullValue) =>
          _result = Some(NullValue)
          
        case ("/", l:IntValue, r:IntValue) =>
          if (r.value == 0) throw new RuntimeException("Zero divide")
          val res = l.value / r.value
          _result = Some(IntValue(res.toString,res,true))
        case ("/", l:DoubleValue, r:DoubleValue) =>
          if (r.value == 0) throw new RuntimeException("Zero divide")
          val res = l.value / r.value
          _result = Some(DoubleValue(res.toString,res,true))
        case ("/", l:IntValue, r:DoubleValue) =>
          if (r.value == 0) throw new RuntimeException("Zero divide")
          val res = l.value / r.value
          _result = Some(DoubleValue(res.toString,res,true))
        case ("/", l:DoubleValue, r:IntValue) =>
          if (r.value == 0) throw new RuntimeException("Zero divide")
          val res = l.value / r.value
          _result = Some(DoubleValue(res.toString,res,true))
        case ("/", NullValue, _) | ("/", _, NullValue) =>
          _result = Some(NullValue)

        case ("<<", l:IntValue, r:IntValue) =>
          val res = l.value << r.value
          _result = Some(IntValue(res.toString,res,true))
        case (">>", l:IntValue, r:IntValue) =>
          val res = l.value >>> r.value
          _result = Some(IntValue(res.toString,res,true))
        case ("|", l:IntValue, r:IntValue) =>
          val res = l.value | r.value
          _result = Some(IntValue(res.toString,res,true))
        case ("&", l:IntValue, r:IntValue) =>
          val res = l.value & r.value
          _result = Some(IntValue(res.toString,res,true))
        case ("^", l:IntValue, r:IntValue) =>
          val res = l.value ^ r.value
          _result = Some(IntValue(res.toString,res,true))

        case _ =>
          throw new RuntimeException(s"Invalid operator between the types:$op")
      }
    }
    this
  }

  override def asValue: Value =
    _result.getOrElse(this.eval.asValue)
  override def spannerType: Type = _result.map(_.spannerType).orNull
}

case class BooleanExpressionValue(op:String,left:Value,right:Value) extends Value {
  private var _result:Option[Value] = None
  override def text = s"${left.text} $op ${right.text}"
  override def isExpression: Boolean = true
  override def isBoolean:Boolean = true

  override def eval: Value = {
    if (_result.isEmpty) {
      (op, left.eval.asValue, if (right != null) right.eval.asValue else right) match {
        case ("<",l:IntValue,r:IntValue) =>
          _result = Some(BooleanValue(l.value < r.value))
        case ("<",l:DoubleValue,r:DoubleValue) =>
          _result = Some(BooleanValue(l.value < r.value))
        case ("<",l:IntValue,r:DoubleValue) =>
          _result = Some(BooleanValue(l.value < r.value))
        case ("<",l:DoubleValue,r:IntValue) =>
          _result = Some(BooleanValue(l.value < r.value))

        case (">",l:IntValue,r:IntValue) =>
          _result = Some(BooleanValue(l.value > r.value))
        case (">",l:DoubleValue,r:DoubleValue) =>
          _result = Some(BooleanValue(l.value > r.value))
        case (">",l:IntValue,r:DoubleValue) =>
          _result = Some(BooleanValue(l.value > r.value))
        case (">",l:DoubleValue,r:IntValue) =>
          _result = Some(BooleanValue(l.value > r.value))

        case ("<=",l:IntValue,r:IntValue) =>
          _result = Some(BooleanValue(l.value <= r.value))
        case ("<=",l:DoubleValue,r:DoubleValue) =>
          _result = Some(BooleanValue(l.value <= r.value))
        case ("<=",l:IntValue,r:DoubleValue) =>
          _result = Some(BooleanValue(l.value <= r.value))
        case ("<=",l:DoubleValue,r:IntValue) =>
          _result = Some(BooleanValue(l.value <= r.value))

        case (">=",l:IntValue,r:IntValue) =>
          _result = Some(BooleanValue(l.value >= r.value))
        case (">=",l:DoubleValue,r:DoubleValue) =>
          _result = Some(BooleanValue(l.value >= r.value))
        case (">=",l:IntValue,r:DoubleValue) =>
          _result = Some(BooleanValue(l.value >= r.value))
        case (">=",l:DoubleValue,r:IntValue) =>
          _result = Some(BooleanValue(l.value >= r.value))

        case ("=",l:IntValue,r:IntValue) =>
          _result = Some(BooleanValue(l.value == r.value))
        case ("=",l:DoubleValue,r:DoubleValue) =>
          _result = Some(BooleanValue(l.value == r.value))
        case ("=",l:IntValue,r:DoubleValue) =>
          _result = Some(BooleanValue(l.value == r.value))
        case ("=",l:DoubleValue,r:IntValue) =>
          _result = Some(BooleanValue(l.value == r.value))

        case ("!=",l:IntValue,r:IntValue) =>
          _result = Some(BooleanValue(l.value != r.value))
        case ("!=",l:DoubleValue,r:DoubleValue) =>
          _result = Some(BooleanValue(l.value != r.value))
        case ("!=",l:IntValue,r:DoubleValue) =>
          _result = Some(BooleanValue(l.value != r.value))
        case ("!=",l:DoubleValue,r:IntValue) =>
          _result = Some(BooleanValue(l.value != r.value))


        case ("&&",l:BooleanValue,r:BooleanValue) =>
          _result = Some(BooleanValue(l.f && r.f))
        case ("||",l:BooleanValue,r:BooleanValue) =>
          _result = Some(BooleanValue(l.f || r.f))
        case ("!",l:BooleanValue,_) =>
          _result = Some(BooleanValue(!l.f))

        case _ =>
          throw new RuntimeException(s"Invalid operator between the types:$op")
      }
    }
    this
  }
  override def asValue: Value =
    _result.getOrElse(this.eval.asValue)
  override def spannerType: Type = _result.map(_.spannerType).orNull
}

case class SubqueryValue(nat:Nat,subquery:String) extends Value {
  import Bolt._

  private var _result:Option[List[Struct]] = None
  private var _isArray = false
  private var _numColumns = 0
  private var _arrayType:Type = null

  override def text = subquery

  private def _getColumn(st:Struct,idx:Int):Value =
    if (st.isNull(idx)) {
      NullValue
    } else {
      st.getColumnType(idx) match {
        case v if v == Type.bool() =>
          BooleanValue(st.getBoolean(idx))
        case v if v == Type.int64() =>
          val i = st.getLong(idx)
          IntValue(i.toString,i,true)
        case v if v == Type.float64() =>
          val d = st.getDouble(idx)
          DoubleValue(d.toString,d,true)
        case v if v == Type.string() =>
          StringValue(st.getString(idx))
        case v if v == Type.timestamp() =>
          TimestampValue(st.getTimestamp(idx).toString)
        case v if v == Type.date() =>
          DateValue(st.getDate(idx).toString)
  //      case v if v == Type.bytes() =>
        case v if v == Type.array(Type.bool()) =>
          ArrayValue(st.getBooleanArray(idx).map(b=>BooleanValue(b).asValue).toList.asJava,true,Type.bool())
        case v if v == Type.array(Type.int64()) =>
          ArrayValue(st.getLongArray(idx).map(l=>IntValue(l.toString,l,true).asValue).toList.asJava,true,Type.int64())
        case v if v == Type.array(Type.float64()) =>
          ArrayValue(st.getDoubleArray(idx).map(f=>DoubleValue(f.toString,f,true).asValue).toList.asJava,true,Type.float64())
        case v if v == Type.array(Type.string()) =>
          ArrayValue(st.getStringList(idx).map(s=>StringValue(s).asValue).toList.asJava,true,Type.string())
        case v if v == Type.array(Type.timestamp()) =>
          ArrayValue(st.getTimestampList(idx).map(t=>TimestampValue(t.toString).asValue).toList.asJava,true,Type.timestamp())
        case v if v == Type.array(Type.date()) =>
          ArrayValue(st.getDateList(idx).map(d=>DateValue(d.toString).asValue).toList.asJava,true,Type.date())
      }
    }

  private def _isArrayType(t:Type):Boolean =
    t match {
      case v if v == Type.array(Type.bool()) =>
        true
      case v if v == Type.array(Type.int64()) =>
        true
      case v if v == Type.array(Type.float64()) =>
        true
      case v if v == Type.array(Type.string()) =>
        true
      case v if v == Type.array(Type.timestamp()) =>
        true
      case v if v == Type.array(Type.date()) =>
        true
      case _ =>
        false
    }

  private def _equalsAllColumnType(st:Struct):Boolean = {
    val c = st.getColumnCount
    _numColumns = c
    if (c > 0) {
      val t = st.getColumnType(0)
      _arrayType = t
      _isArray = _isArrayType(t)
      (1 until c).forall(st.getColumnType(_) == t)
    } else { true }
  }

  override def eval:Value = {
    if (_result.isEmpty) {
      val r = nat.executeNativeQuery(subquery).autoclose(
        _.map(_.getCurrentRowAsStruct).toList)
      if (r.length > 1 && _numColumns > 1)
        throw new RuntimeException(s"The subquery has multi rows:$subquery")

      r.headOption.foreach(
        row =>
          if (!_equalsAllColumnType(row))
            throw new RuntimeException
      )
      _result = Some(r)
    }
    this
  }

  def isArray:Boolean = _isArray

  override def asValue:Value = {
    val r = _result.get
    if (r.length > 1)
      throw new RuntimeException(s"The subquery has multi rows:$subquery")
    _numColumns match {
      case 0 =>
        NullValue
      case 1 =>
        val r = _getColumn(_result.get.head,0)
        r
      case _ =>
        throw new RuntimeException(s"The subquery has multi columns:$subquery")
    }
  }

  override def asArray:ArrayValue = {
    val r = _result.get

    (r.length, _numColumns) match {
      case (_, 0) | (0, _) =>
        ArrayValue(List())
      case (1,1) =>
        val st = r.head
        if (_isArray) {
          _getColumn(st,0).asInstanceOf[ArrayValue]
        } else {
          ArrayValue(List(_getColumn(st,0)).filter(_ != NullValue),true,_arrayType)
        }
      case (1,_) =>
        if (_isArray)
          throw new RuntimeException("Nested Array is not supported")
        val st = r.head
        ArrayValue((0 until _numColumns).map(i => _getColumn(st, i)).toList.filter(_ != NullValue).asJava,true,_arrayType)
      case (_,1) =>
        if (_isArray)
          throw new RuntimeException("Nested Array is not supported")
        ArrayValue(r.map(st => _getColumn(st, 0)).filter(_ != NullValue).asJava,true,_arrayType)
      case _ =>
        throw new RuntimeException(s"The subquery could not cast to array:$subquery")
    }
  }
}
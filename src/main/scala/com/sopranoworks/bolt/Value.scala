package com.sopranoworks.bolt

import com.google.cloud.spanner.{DatabaseClient, ResultSet, Struct, Type}
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
}

case class StringValue(text:String) extends Value {
  override def qtext:String = s"'$text'"
}

case class IntValue(text:String) extends Value

case object NullValue extends Value {
  override def text: String = "NULL"
}

case class ArrayValue(values:java.util.List[String]) extends Value  {
  override def text:String = s"[${values.asScala.mkString(",")}]"
}

case class FunctionValue(name:String,parameters:List[Value]) extends Value {
  override def text = s"$name(${parameters.map(_.text).mkString(",")})"
}

case class StructValue(var members:List[Value] = Nil) extends Value {
  override def text = s"STRUCT(${members.map(_.text).mkString(",")})"

  def addValue(v:Value):Unit = {
    members :+= v
  }
}

case class ArrayExpressionValue(values:List[Value]) extends Value {
  override def text = s"[${values.map(_.text).mkString(",")}]"
  override def eval: Value = {
    values.foreach(_.eval)
    this
  }
}

case class ExpressionValue(op:String,left:Value,right:Value) extends Value {
  override def text = s"${left.text} $op ${right.text}"

  override def eval: Value = throw new RuntimeException("Expression is not supported yet")
}

case class SubqueryValue(nat:Nat,subquery:String) extends Value {
  import Bolt._

  private var _result:Option[List[Struct]] = None
  private var _isArray = false
  private var _numColumns = 0

  override def text = subquery

  private def _getColumn(st:Struct,idx:Int):Value = {
    st.getColumnType(idx) match {
      case v if v == Type.bool() =>
        IntValue(st.getBoolean(idx).toString)
      case v if v == Type.int64() =>
        IntValue(st.getLong(idx).toString)
      case v if v == Type.float64() =>
        IntValue(st.getDouble(idx).toString)
      case v if v == Type.string() =>
        StringValue(st.getString(idx))
      case v if v == Type.timestamp() =>
        StringValue(st.getTimestamp(idx).toString)
      case v if v == Type.date() =>
        StringValue(st.getDate(idx).toString)
      case v if v == Type.array(Type.bool()) =>
        ArrayValue(st.getBooleanArray(idx).map(_.toString).toList.asJava)
      case v if v == Type.array(Type.int64()) =>
        ArrayValue(st.getLongArray(idx).map(_.toString).toList.asJava)
      case v if v == Type.array(Type.float64()) =>
        ArrayValue(st.getDoubleArray(idx).map(_.toString).toList.asJava)
      case v if v == Type.array(Type.string()) =>
        ArrayValue(st.getStringList(idx).map(_.toString).toList.asJava)
      case v if v == Type.array(Type.timestamp()) =>
        ArrayValue(st.getTimestampList(idx).map(_.toString).toList.asJava)
      case v if v == Type.array(Type.date()) =>
        ArrayValue(st.getDateList(idx).map(_.toString).toList.asJava)
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
      _isArray = _isArrayType(t)
      (1 until c).forall(st.getColumnType(_) == t)
    } else { true }
  }

  override def eval:Value = {
    val r = nat.executeNativeQuery(subquery).autoclose(
          _.map(_.getCurrentRowAsStruct).toList)
    if (r.length > 1)
      throw new RuntimeException(s"Subquery has multi rows:$subquery")

    r.headOption.foreach(
      row =>
        if (!_equalsAllColumnType(row))
          throw new RuntimeException
    )
    _result = Some(r)
    this
  }

  def isArray:Boolean = _isArray

  override def asValue:Value = {
    _numColumns match {
      case 0 =>
        NullValue
      case 1 =>
        val r = _getColumn(_result.get.head,0)
//        println(r)
        r
      case _ =>
        throw new RuntimeException(s"Subquery has multi columns:$subquery")
    }
  }

  def asArray:ArrayValue = {
    _numColumns match {
      case 0 =>
        ArrayValue(List())
      case _ =>
        val st = _result.get.head
        ArrayValue((0 until _numColumns).map(i =>_getColumn(st,i).text).toList.asJava)
    }
  }
}
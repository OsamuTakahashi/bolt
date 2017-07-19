package com.sopranoworks.bolt

import com.google.cloud.Timestamp
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
  def isExpression:Boolean = false
  def isBoolean:Boolean = false
}

case object NullValue extends Value {
  override def text: String = "NULL"
}

//case object TrueValue extends Value {
//  override def text: String = "TRUE"
//}
//
//case object FalseValue extends Value {
//  override def text: String = "FALSE"
//}

case class StringValue(text:String) extends Value {
  override def qtext:String = s"'$text'"
}

case class BooleanValue(f:Boolean) extends Value {
  override def text:String = if (f) "TRUE" else "FALSE"
  override def isBoolean:Boolean = true
}

case class IntValue(text:String,var value:Long = 0,var evaluated:Boolean = false) extends Value {
  override def eval: Value = {
    if (!evaluated) {
      value = java.lang.Long.parseLong(text)
      evaluated = true
    }
    this
  }
}

case class DoubleValue(text:String,var value:Double = 0,var evaluated:Boolean = false) extends Value {
  override def eval: Value = {
    if (!evaluated) {
      value = java.lang.Double.parseDouble(text)
      evaluated = true
    }
    this
  }
}

case class TimestampValue(text:String) extends Value {
  override def qtext:String = s"'$text'"
}

case class DateValue(text:String) extends Value {
  override def qtext:String = s"'$text'"
}

case class ArrayValue(values:java.util.List[String]) extends Value  {
  override def text:String = s"[${values.asScala.mkString(",")}]"
}

case class FunctionValue(name:String,parameters:java.util.List[Value]) extends Value {
  private var _result:Option[Value] = None
  override def text = s"$name(${parameters.map(_.text).mkString(",")})"
  override def eval: Value = {
    name match {
      case "NOW" =>
        if (parameters.nonEmpty)
          throw new RuntimeException("Function NOW takes no parameter")
        val ts = Timestamp.now()
        _result = Some(TimestampValue(ts.toString))

      case "IF" =>
        if (parameters.length != 3)
          throw new RuntimeException("Function IF takes 3 parameters")
        val cond = parameters.get(0).eval.asValue
        cond match {
          case BooleanValue(f) =>
            _result = Some((if (f) parameters.get(1) else parameters(2)).eval.asValue)
          case _ =>
            if (!cond.isBoolean)
              throw new RuntimeException("The first paramter of function IF must be boolean expression")

        }


      case _ =>
        throw new RuntimeException(s"Unevaluatable function $name")
    }
    this
  }


}

case class StructValue() extends Value {
  private var members:List[Value] = Nil
  override def text = s"STRUCT(${members.map(_.text).mkString(",")})"
  override def eval: Value = throw new RuntimeException("Struct is not supported yet")

  def addValue(v:Value):Unit = {
    members :+= v
  }
}

case class ArrayExpressionValue(var values:List[Value]) extends Value {
  override def text = s"[${values.map(_.text).mkString(",")}]"
  override def eval: Value = {
    values = values.map(_.eval.asValue)
    this
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
      case v if v == Type.array(Type.bool()) =>
        ArrayValue(st.getBooleanArray(idx).map(_.toString).toList.asJava)
      case v if v == Type.array(Type.int64()) =>
        ArrayValue(st.getLongArray(idx).map(_.toString).toList.asJava)
      case v if v == Type.array(Type.float64()) =>
        ArrayValue(st.getDoubleArray(idx).map(_.toString).toList.asJava)
      case v if v == Type.array(Type.string()) =>
        ArrayValue(st.getStringList(idx).toList.asJava)
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
    if (_result.isEmpty) {
      val r = nat.executeNativeQuery(subquery).autoclose(
        _.map(_.getCurrentRowAsStruct).toList)
      if (r.length > 1)
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
    _numColumns match {
      case 0 =>
        NullValue
      case 1 =>
        val r = _getColumn(_result.get.head,0)
//        println(r)
        r
      case _ =>
        throw new RuntimeException(s"The subquery has multi columns:$subquery")
    }
  }

  def asArray:ArrayValue =
    if (_isArray) {
      _numColumns match {
        case 0 =>
          ArrayValue(List())
        case _ =>
          val st = _result.get.head
          ArrayValue((0 until _numColumns).map(i =>_getColumn(st,i).text).toList.asJava)
      }
    } else {
      throw new RuntimeException(s"The result of the subquery is not array:$subquery")
    }
}
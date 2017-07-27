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

import com.google.cloud.{Date, Timestamp}
import com.google.cloud.spanner.{Mutation, Struct, Type}
import com.sopranoworks.bolt.Bolt.Nat
import org.joda.time.DateTime

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
  * Created by takahashi on 2017/03/29.
  */
trait Value {
  def text:String
  def qtext:String = text

  def eval:Value = this

  def invalidateEvaluatedValueIfContains(values:List[Value]):Boolean = false
//  def contains(values:List[Value]):Boolean = false

  def asValue:Value = this
  def isBoolean:Boolean = false
  def spannerType:Type = null

  def asArray:ArrayValue = throw new RuntimeException("Could not treat the value as Array")

  def setTo(m:Mutation.WriteBuilder,key:String):Unit = {
    throw new RuntimeException("The value is not storable")
  }

  def isEqualValue(v:Value):Boolean = false

  def apply():Value = this

  def resolveReference(columns:Map[String,TableColumnValue] = Map.empty[String,TableColumnValue]):Map[String,TableColumnValue] = columns
  def getField(fieldIdx:Int):Value = throw new RuntimeException(s"The value '$text' does not have a field #$fieldIdx.")
  def getField(fieldName:String):Value = throw new RuntimeException(s"The value '$text' does not have a field '$fieldName'.")
}

trait WrappedValue extends Value {
  protected var _ref:Option[Value] = None

  override def asValue: Value = _ref.map(_.eval.asValue).getOrElse(this)
  override def asArray: ArrayValue = _ref.map(_.eval.asArray).getOrElse(super.asArray)

  override def spannerType: Type = _ref.map(_.spannerType).orNull

  override def eval: Value = _ref.map(_.eval).getOrElse(this)

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

case object NullValue extends Value {
  override def text: String = "NULL"
  override def setTo(m: Mutation.WriteBuilder, key: String): Unit = {}
  override def isEqualValue(v:Value):Boolean = v == NullValue
}

case class StringValue(text:String) extends Value with TextSetter {
  override def qtext:String = s"'$text'"
  override def spannerType: Type = Type.string()
  override def isEqualValue(v:Value):Boolean =
    v.isInstanceOf[StringValue] && v.asInstanceOf[StringValue].text == text
}

case class BooleanValue(f:Boolean) extends Value with TextSetter {
  override def text:String = if (f) "TRUE" else "FALSE"
  override def isBoolean:Boolean = true
  override def spannerType: Type = Type.bool()
  override def isEqualValue(v:Value):Boolean =
    v.isInstanceOf[BooleanValue] && v.asInstanceOf[BooleanValue].f == f
}

case class IntValue(text:String,var value:Long = 0,var evaluated:Boolean = false) extends Value with TextSetter {
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

case class DoubleValue(text:String,var value:Double = 0,var evaluated:Boolean = false) extends Value with TextSetter {
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

case class TimestampValue(text:String) extends Value with TextSetter {
  override def qtext:String = s"'$text'"
  override def spannerType: Type = Type.timestamp()
  override def isEqualValue(v:Value):Boolean =
    v.isInstanceOf[TimestampValue] && v.asInstanceOf[TimestampValue].text == text
}

case class DateValue(text:String) extends Value with TextSetter  {
  override def qtext:String = s"'$text'"
  override def spannerType: Type = Type.date()
  override def isEqualValue(v:Value):Boolean =
    v.isInstanceOf[DateValue] && v.asInstanceOf[DateValue].text == text
}

case class BytesValue(text:String) extends Value {
  override def spannerType: Type = Type.bytes()
}

case class ArrayValue(var values:java.util.List[Value],var evaluated:Boolean = false,var arrayType:Type = null) extends Value  {
  private var _evaluated = List.empty[Value]

  private def _isValidArray:Boolean = {
    if (_evaluated.isEmpty) {
      true
    } else {
      val t = if (arrayType != null) arrayType else _evaluated(0).spannerType
      if (t != null) {
        arrayType = t
        _evaluated.forall(_.spannerType == t)
      } else {
        false
      }
    }
  }

  override def eval: Value = {
    if (!evaluated || values.length != _evaluated.length) {
      _evaluated = values.toList.map(_.eval.asValue).filter(_ != NullValue)
      if (!_isValidArray)
        throw new RuntimeException("")
      evaluated = true
    }
    this
  }

  override def invalidateEvaluatedValueIfContains(values: List[Value]): Boolean = {
    if (values.foldLeft(false) {
      case (f,v) => f || v.invalidateEvaluatedValueIfContains(values)
    }) {
      evaluated = false
      _evaluated = List.empty[Value]
      true
    } else false
  }

  override def resolveReference(columns: Map[String, TableColumnValue]): Map[String, TableColumnValue] = {
    values.foldLeft(columns) {
      case (c,v) =>
        v.resolveReference(c)
    }
  }

  override def text:String = values.map(_.text).mkString("[",",","]")
  override def spannerType: Type = {
    eval
    arrayType
  }

  override def asArray: ArrayValue = this

  def length:Int = {
    eval
    _evaluated.length
  }

  def offset(v:Value):Value = {
    val n = v.eval.asValue match {
      case NullValue =>
        0
      case IntValue(_,i,_) =>
        i.toInt
      case _ =>
        throw new RuntimeException("Array offset type must be int64")
    }
    getField(n)
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
    getField(n)
  }

  def contains(v:Value):Boolean = {
    this.eval
    _evaluated.exists(_.isEqualValue(v))
  }

  override def setTo(m: Mutation.WriteBuilder, key: String): Unit = {
    this.eval
    m.set(key).toStringArray(values.map(_.text))
  }

  override def getField(fieldIdx:Int):Value = {
    if (fieldIdx < 0 || values.length <= fieldIdx) throw new ArrayIndexOutOfBoundsException
    values.get(fieldIdx)
  }
}

/**
  *
  * @param name
  * @param qc
  */
case class IdentifierValue(name:String,qc:QueryContext) extends WrappedValue {
  override def text: String = name

  override def resolveReference(columns: Map[String,TableColumnValue] = Map.empty[String,TableColumnValue]): Map[String,TableColumnValue] = {
    var res = columns
    qc.getAlias(name) match {
      case Some(alias) =>
        alias match {
          case TableAlias(_,tableName) =>
            _ref = Some(TableValue(tableName,qc))
          case QueryResultAlias(_,i,q) =>
            q.subquery match {
              case Some(sq) =>
                _ref = Some(ResultIndexValue(sq,i))
              case None =>
                _ref = Some(NullValue)  // TODO: check this
            }
          case ExpressionAlias(_,v) =>
            _ref = Some(v)
        }

      case None =>
        qc.currentTable.flatMap(tableName=>qc.nat.database.table(tableName).flatMap(_.columnIndexOf(name).map((tableName,_)))) match {
          case Some((tableName,idx)) =>
            val key = s"$tableName.$name"
            columns.get(key) match {
              case r @ Some(_) =>
                _ref = r
              case None =>
                val v = TableColumnValue(name,tableName,idx)
                _ref = Some(v)
                res += key->v
            }

          case None =>
            qc.nat.database.table(name) match {
              case Some(tbl) =>
                _ref = Some(TableValue(name,qc))
              case None =>
                throw new RuntimeException(s"Unresolvable identifier $name")
            }
        }
    }
    res
  }

  override def eval: Value = {
    if (_ref.isEmpty) {
      this.resolveReference(Map())
    }
    this
  }

  override def asValue: Value = {
    this.eval
    if (_ref.isEmpty) {
      this.resolveReference(Map())
      if (_ref.isEmpty)
        throw new RuntimeException(s"Unresolvable identifier $name")
    }
    _ref.get.eval.asValue
  }
}

/**
  *
  */
case class IdentifierWithFieldValue(name:String,fields:java.util.List[String],qc:QueryContext) extends WrappedValue {
  override def text: String = s"$name.${fields.mkString(".")}"

  override def resolveReference(columns: Map[String,TableColumnValue] = Map.empty[String,TableColumnValue]): Map[String,TableColumnValue] = {
    if (_ref.isEmpty) {
      val v = IdentifierValue(name, qc)
      val vv = fields.foldLeft[Value](v) {
        case (v, k) =>
          ResultFieldValue(v, k)
      }
      _ref = Some(vv)
    }
    _ref.get.resolveReference(columns)
  }

  override def eval: Value = {
    if (_ref.isEmpty) {
      this.resolveReference(Map())
    }
    _ref.get.eval
    this
  }
}

case class SubfieldValue(value:Value,fieldNames:List[String]) extends WrappedValue {
  override def text: String = s"${value.text}.${fieldNames.mkString(",")}"

  override def eval: Value = {
    if (_ref.isEmpty) {
      _ref = Some(fieldNames.foldLeft(value) {
        case (v, name) =>
          v.getField(name)
      })
    }
    this
  }
}

/**
  *
  */
case class TableValue(name:String,qc:QueryContext) extends Value {
  override def text: String = name

  //  override def eval: Value = throw new RuntimeException("A table alias is not able to used in expression")
  override def getField(fieldName: String): Value =
    (for {
      tbl<-qc.nat.database.table(name)
      col<-tbl.columns.find(_.name == fieldName)
    } yield {
      TableColumnValue(fieldName,name,col.position)
    }).getOrElse(throw new RuntimeException(s"Table $name does not have a field named '$fieldName'"))
}


/**
  *
  * @note This value will be only made from IdentifierValud's resolveReference method.
  */
case class TableColumnValue(name:String,tableName:String,index:Int) extends WrappedValue {
  def setValue(v:Value):Unit = _ref = Some(v)

  override def text: String = s"$tableName.$name"
  override def eval: Value =
    _ref match {
      case Some(v) => v
      case None =>
        this
    }

  override def resolveReference(columns: Map[String,TableColumnValue] = Map.empty[String,TableColumnValue]): Map[String,TableColumnValue] = {
    val key = s"$tableName.$name"
    if (!columns.contains(key)) columns + (key->this) else columns
  }


  override def invalidateEvaluatedValueIfContains(values: List[Value]): Boolean = {
    values.contains(this)
  }

  override def asValue: Value = {
    this.eval
    if (_ref.isEmpty) {
      this.resolveReference(Map())
      if (_ref.isEmpty)
        throw new RuntimeException(s"Unresolvable table column identifier $name")
    }
    _ref.get.eval.asValue
  }
}

case class ResultIndexValue(expression:Value,index:Int) extends WrappedValue {
  override def text = s"(${expression.text})[$index]"

  override def eval: Value = {
    if (_ref.isEmpty) {
      _ref = Some(expression.eval.getField(index).eval)
    }
    this
  }
}

case class ResultFieldValue(value:Value, fieldName:String) extends WrappedValue {
  override def text: String = s"${value.text}.$fieldName"

  override def eval: Value = {
    if (_ref.isEmpty) {
      _ref = Some(value.eval.getField(fieldName).eval)
    }
    this
  }
}


/**
  *
  */
case class FunctionValue(name:String,parameters:java.util.List[Value]) extends WrappedValue {

  override def text = s"$name(${parameters.map(_.text).mkString(",")})"
  override def eval: Value = {
    name match {
      case "NOW" =>
        if (parameters.nonEmpty)
          throw new RuntimeException("Function NOW takes no parameter")
        val ts = Timestamp.now()
        _ref = Some(TimestampValue(ts.toString))

      case "COALESCE" =>
        if (parameters.isEmpty)
          throw new RuntimeException("Function COALESCE takes least 1 parameter")
        _ref = parameters.find(_.eval.asValue != NullValue).orElse(Some(NullValue))
      case "IF" =>
        if (parameters.length != 3)
          throw new RuntimeException("Function IF takes 3 parameters")
        val cond = parameters.get(0).eval.asValue
        cond match {
          case BooleanValue(f) =>
            _ref = Some((if (f) parameters.get(1) else parameters(2)).eval.asValue)
          case _ =>
            if (!cond.isBoolean)
              throw new RuntimeException("The first parameter of function IF must be boolean expression")
        }
      case "IFNULL" =>
        if (parameters.length != 2)
          throw new RuntimeException("Function IF takes 2 parameters")
        parameters.get(0).eval.asValue match {
          case NullValue =>
            _ref = Some(parameters.get(1))
          case v =>
            _ref = Some(v)
        }
        
        // Array functions
      case "ARRAY_LENGTH" =>
        if (parameters.length != 1)
          throw new RuntimeException("Function ARRAY_LENGTH takes 1 parameter")

        parameters.get(0).eval.asValue match {
          case arr:ArrayValue =>
            val l = arr.length
            _ref = Some(IntValue(l.toString,l,true))
          case _ =>
            throw new RuntimeException("The parameter type of ARRAY_LENGTH must be ARRAY")
        }


        // internal functions
      case "$ARRAY" =>
        if (parameters.length != 1)
          throw new RuntimeException("Function $ARRAY takes 1 parameter")

        _ref = Some(parameters.head match {
          case v:ArrayValue =>
            v
          case q:SubqueryValue =>
            q.eval.asArray
          case _ =>
            throw new RuntimeException("Could not convert parameters to an array")
        })

      case "$OFFSET" =>
        if (parameters.length != 2)
          throw new RuntimeException("Function $OFFSET takes 1 parameter")
        (parameters.get(0).eval.asValue,parameters.get(1).eval.asValue) match {
          case (arr:ArrayValue,i:IntValue) =>
            _ref = Some(arr.offset(i))
          case (_,_:IntValue) =>
            throw new RuntimeException("Array element access with non-array value")
          case (_:ArrayValue,_) =>
            throw new RuntimeException("Index type of array element access must be INT64")
          case (_,_) =>
            throw new RuntimeException("Invalid operation")
        }
        
      case "$ORDINAL" =>
        if (parameters.length != 2)
          throw new RuntimeException("Function $ORDINAL takes 1 parameter")
        (parameters.get(0).eval.asValue,parameters.get(1).eval.asValue) match {
          case (arr:ArrayValue,i:IntValue) =>
            _ref = Some(arr.ordinal(i))
          case (_,_:IntValue) =>
            throw new RuntimeException("Array element access with non-array value")
          case (_:ArrayValue,_) =>
            throw new RuntimeException("Index type of array element access must be INT64")
          case (_,_) =>
            throw new RuntimeException("Invalid operation")
        }
      case "$EXISTS" =>
        if (parameters.length != 1)
          throw new RuntimeException("Function $EXISTS takes 1 parameter")
        parameters.get(0).eval match {
          case q : SubqueryValue =>
            _ref = Some(BooleanValue(q.numRows > 0))
          case _ =>
            throw new RuntimeException("Invalid operation at EXISTS")
        }

      case "$LIKE" =>
        if (parameters.length != 2)
          throw new RuntimeException("Function $LIKE takes 2 parameter")
        (parameters.get(0).eval.asValue,parameters.get(1).eval.asValue) match {
          case (a:StringValue,b:StringValue) =>
            _ref = Some(BooleanValue(LikeMatcher(b.text).findFirstIn(a.text).isDefined))
              
          case (_,_) =>
            throw new RuntimeException("Invalid operation at LIKE")
        }

      case "$IN" =>
        if (parameters.length != 2)
          throw new RuntimeException("Function $IN takes 2 parameter")
        (parameters.get(0).eval.asValue,parameters.get(1).eval.asArray) match {
          case (v,arr:ArrayValue) =>
            _ref = Some(BooleanValue(if (arr.length > 0 && v.spannerType == arr.spannerType && v.spannerType != null) {
              arr.contains(v)
            } else false))

          case (_,_) =>
            throw new RuntimeException("Invalid operation at LIKE")
        }

      case "$BETWEEN" =>
        if (parameters.length != 3)
          throw new RuntimeException("Function $BETWEEN takes 3 parameter")
        val v = parameters.get(0).eval.asValue
        val a = parameters.get(1).eval.asValue
        val b = parameters.get(2).eval.asValue

        try {
          _ref = Some(BooleanExpressionValue("&&", BooleanExpressionValue("<=", a, v), BooleanExpressionValue("<=", v, b)).eval.asValue)
        } catch {
          case _ : RuntimeException =>
            throw new RuntimeException("Invalid operation in BETWEEN")
        }
        
      case _ =>
        throw new RuntimeException(s"Unevaluatable function $name")
    }
    this
  }

  override def invalidateEvaluatedValueIfContains(values:List[Value]):Boolean = {
    if (parameters.foldLeft(false) {
      case (f,v) =>
        f || v.invalidateEvaluatedValueIfContains(values)
    }) {
      _ref = None
      true
    } else false
  }


  override def resolveReference(columns:Map[String,TableColumnValue]): Map[String,TableColumnValue] = {
    parameters.foldLeft(columns){
      case (c,p) =>
        p.resolveReference(c)
    }
  }
}

/**
  *
  */
case class StructValue() extends Value {
  private var _evaluated = false
  private var _members:List[Value] = Nil
  private var _aliases = Map.empty[String,Int]

  override def text = s"STRUCT(${_members.map(_.text).mkString(",")})"

  override def eval: Value = {
    if (!_evaluated) {
      _members = _members.map(_.eval.asValue)
      _evaluated = true
    }
    this
  }

  def addValue(v:Value):Unit = {
    _members :+= v
  }
  def addFieldName(name:String,idx:Int):Unit = {
    if (_aliases.contains(name))
      throw new RuntimeException(s"Duplicate field name $name")
    _aliases += (name->idx)
  }

  override def getField(fieldIdx: Int): Value = {
    if (fieldIdx < 0 || _members.length <= fieldIdx)
      throw new RuntimeException("THe specified filed index is out of range")
    _members(fieldIdx)
  }

  override def getField(fieldName: String): Value = {
    _aliases.get(fieldName) match {
      case Some(idx) => getField(idx)
      case None => throw new RuntimeException(s"The struct does not have field named '$fieldName'")
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

  override def asArray: ArrayValue = {
    this.eval
    _members match {
      case Nil =>
        ArrayValue(List())
      case head :: tail =>
        if (!_isArrayType(head.spannerType) || !_members.forall(_.spannerType == head.spannerType))
          throw new RuntimeException("The struct can not convert to an array")
        ArrayValue(_members,true,head.spannerType)
    }
  }

  override def invalidateEvaluatedValueIfContains(values:List[Value]):Boolean = {
    _members.foldLeft(false) {
      case (f,v) => f || v.invalidateEvaluatedValueIfContains(values)
    }
  }
}

/**
  *
  * @param op operator
  * @param left left expression value
  * @param right right expression value
  */
case class ExpressionValue(op:String,left:Value,right:Value) extends WrappedValue {
  override def text = s"(${left.text} $op ${right.text})"
  override def eval: Value = {
    if (_ref.isEmpty) {
      (op, left.eval.asValue, if (right != null) right.eval.asValue else right) match {
        case ("-", l:IntValue, null) =>
          val v = -l.value
          _ref = Some(IntValue(v.toString,v,true))
        case ("-", l:DoubleValue, null) =>
          val v = -l.value
          _ref = Some(DoubleValue(v.toString,v,true))
        case ("~", l:IntValue, null) =>
          val v = ~l.value
          _ref = Some(IntValue(v.toString,v,true))

        case ("+", l:IntValue, r:IntValue) =>
          val res = l.value + r.value
          _ref = Some(IntValue(res.toString,res,true))
        case ("+", l:DoubleValue, r:DoubleValue) =>
          val res = l.value + r.value
          _ref = Some(DoubleValue(res.toString,res,true))
        case ("+", l:IntValue, r:DoubleValue) =>
          val res = l.value + r.value
          _ref = Some(DoubleValue(res.toString,res,true))
        case ("+", l:DoubleValue, r:IntValue) =>
          val res = l.value + r.value
          _ref = Some(DoubleValue(res.toString,res,true))
        case ("+", NullValue, _) | ("+", _, NullValue) =>
          _ref = Some(NullValue)

        case ("-", l:IntValue, r:IntValue) =>
          val res = l.value - r.value
          _ref = Some(IntValue(res.toString,res,true))
        case ("-", l:DoubleValue, r:DoubleValue) =>
          val res = l.value - r.value
          _ref = Some(DoubleValue(res.toString,res,true))
        case ("-", l:IntValue, r:DoubleValue) =>
          val res = l.value - r.value
          _ref = Some(DoubleValue(res.toString,res,true))
        case ("-", l:DoubleValue, r:IntValue) =>
          val res = l.value - r.value
          _ref = Some(DoubleValue(res.toString,res,true))
        case ("-", NullValue, _) | ("-", _, NullValue) =>
          _ref = Some(NullValue)

        case ("*", l:IntValue, r:IntValue) =>
          val res = l.value * r.value
          _ref = Some(IntValue(res.toString,res,true))
        case ("*", l:DoubleValue, r:DoubleValue) =>
          val res = l.value * r.value
          _ref = Some(DoubleValue(res.toString,res,true))
        case ("*", l:IntValue, r:DoubleValue) =>
          val res = l.value * r.value
          _ref = Some(DoubleValue(res.toString,res,true))
        case ("*", l:DoubleValue, r:IntValue) =>
          val res = l.value * r.value
          _ref = Some(DoubleValue(res.toString,res,true))
        case ("*", NullValue, _) | ("*", _, NullValue) =>
          _ref = Some(NullValue)
          
        case ("/", l:IntValue, r:IntValue) =>
          if (r.value == 0) throw new RuntimeException("Zero divide")
          val res = l.value / r.value
          _ref = Some(IntValue(res.toString,res,true))
        case ("/", l:DoubleValue, r:DoubleValue) =>
          if (r.value == 0) throw new RuntimeException("Zero divide")
          val res = l.value / r.value
          _ref = Some(DoubleValue(res.toString,res,true))
        case ("/", l:IntValue, r:DoubleValue) =>
          if (r.value == 0) throw new RuntimeException("Zero divide")
          val res = l.value / r.value
          _ref = Some(DoubleValue(res.toString,res,true))
        case ("/", l:DoubleValue, r:IntValue) =>
          if (r.value == 0) throw new RuntimeException("Zero divide")
          val res = l.value / r.value
          _ref = Some(DoubleValue(res.toString,res,true))
        case ("/", NullValue, _) | ("/", _, NullValue) =>
          _ref = Some(NullValue)

        case ("<<", l:IntValue, r:IntValue) =>
          val res = l.value << r.value
          _ref = Some(IntValue(res.toString,res,true))
        case (">>", l:IntValue, r:IntValue) =>
          val res = l.value >>> r.value
          _ref = Some(IntValue(res.toString,res,true))
        case ("|", l:IntValue, r:IntValue) =>
          val res = l.value | r.value
          _ref = Some(IntValue(res.toString,res,true))
        case ("&", l:IntValue, r:IntValue) =>
          val res = l.value & r.value
          _ref = Some(IntValue(res.toString,res,true))
        case ("^", l:IntValue, r:IntValue) =>
          val res = l.value ^ r.value
          _ref = Some(IntValue(res.toString,res,true))

        case (_,l,r) =>
          throw new RuntimeException(s"Invalid operator between the types:$l $op $r")
      }
    }
    this
  }

  override def invalidateEvaluatedValueIfContains(values:List[Value]):Boolean = {
    if ((if (left != null) left.invalidateEvaluatedValueIfContains(values) else false) ||
      (if (right != null) right.invalidateEvaluatedValueIfContains(values) else false)) {
      _ref = None
      true
    } else false
  }


  override def resolveReference(columns:Map[String,TableColumnValue] = Map.empty[String,TableColumnValue]): Map[String,TableColumnValue] = {
    val lc = if (left != null) left.resolveReference(columns) else columns
    if (right != null) right.resolveReference(lc) else lc
  }
}

/**
  *
  * @param op boolean operator
  * @param left left expression value
  * @param right right expression value
  */
case class BooleanExpressionValue(op:String,left:Value,right:Value) extends WrappedValue {
  override def text = s"(${left.text} $op ${right.text})"
  override def isBoolean:Boolean = true

  override def eval: Value = {
    if (_ref.isEmpty) {
      (op, left.eval.asValue, if (right != null) right.eval.asValue else right) match {
        case ("<",l:IntValue,r:IntValue) =>
          _ref = Some(BooleanValue(l.value < r.value))
        case ("<",l:DoubleValue,r:DoubleValue) =>
          _ref = Some(BooleanValue(l.value < r.value))
        case ("<",l:IntValue,r:DoubleValue) =>
          _ref = Some(BooleanValue(l.value < r.value))
        case ("<",l:DoubleValue,r:IntValue) =>
          _ref = Some(BooleanValue(l.value < r.value))

        case (">",l:IntValue,r:IntValue) =>
          _ref = Some(BooleanValue(l.value > r.value))
        case (">",l:DoubleValue,r:DoubleValue) =>
          _ref = Some(BooleanValue(l.value > r.value))
        case (">",l:IntValue,r:DoubleValue) =>
          _ref = Some(BooleanValue(l.value > r.value))
        case (">",l:DoubleValue,r:IntValue) =>
          _ref = Some(BooleanValue(l.value > r.value))

        case ("<=",l:IntValue,r:IntValue) =>
          _ref = Some(BooleanValue(l.value <= r.value))
        case ("<=",l:DoubleValue,r:DoubleValue) =>
          _ref = Some(BooleanValue(l.value <= r.value))
        case ("<=",l:IntValue,r:DoubleValue) =>
          _ref = Some(BooleanValue(l.value <= r.value))
        case ("<=",l:DoubleValue,r:IntValue) =>
          _ref = Some(BooleanValue(l.value <= r.value))

        case (">=",l:IntValue,r:IntValue) =>
          _ref = Some(BooleanValue(l.value >= r.value))
        case (">=",l:DoubleValue,r:DoubleValue) =>
          _ref = Some(BooleanValue(l.value >= r.value))
        case (">=",l:IntValue,r:DoubleValue) =>
          _ref = Some(BooleanValue(l.value >= r.value))
        case (">=",l:DoubleValue,r:IntValue) =>
          _ref = Some(BooleanValue(l.value >= r.value))

        case ("=",l:IntValue,r:IntValue) =>
          _ref = Some(BooleanValue(l.value == r.value))
        case ("=",l:DoubleValue,r:DoubleValue) =>
          _ref = Some(BooleanValue(l.value == r.value))
        case ("=",l:IntValue,r:DoubleValue) =>
          _ref = Some(BooleanValue(l.value == r.value))
        case ("=",l:DoubleValue,r:IntValue) =>
          _ref = Some(BooleanValue(l.value == r.value))

        case ("!=",l:IntValue,r:IntValue) =>
          _ref = Some(BooleanValue(l.value != r.value))
        case ("!=",l:DoubleValue,r:DoubleValue) =>
          _ref = Some(BooleanValue(l.value != r.value))
        case ("!=",l:IntValue,r:DoubleValue) =>
          _ref = Some(BooleanValue(l.value != r.value))
        case ("!=",l:DoubleValue,r:IntValue) =>
          _ref = Some(BooleanValue(l.value != r.value))


        case ("&&",l:BooleanValue,r:BooleanValue) =>
          _ref = Some(BooleanValue(l.f && r.f))
        case ("||",l:BooleanValue,r:BooleanValue) =>
          _ref = Some(BooleanValue(l.f || r.f))
        case ("!",l:BooleanValue,_) =>
          _ref = Some(BooleanValue(!l.f))

        case _ =>
          throw new RuntimeException(s"Invalid operator between the types:$op")
      }
    }
    this
  }
  override def invalidateEvaluatedValueIfContains(values:List[Value]):Boolean = {
    if ((if (left != null) left.invalidateEvaluatedValueIfContains(values) else false) ||
      (if (right != null) right.invalidateEvaluatedValueIfContains(values) else false)) {
      _ref = None
      true
    } else false
  }

  override def resolveReference(columns:Map[String,TableColumnValue]): Map[String,TableColumnValue] = {
    val lc = if (left != null) left.resolveReference(columns) else columns
    if (right != null) right.resolveReference(lc) else lc
  }
}

case class CastValue(expression:Value,toType:Type) extends Value with WrappedValue {
  override def text: String = s"CAST(${expression.text} AS $toType)"
  override def eval: Value = {
    if (_ref.isEmpty) {
      toType match {
        case tp if tp == Type.bool() =>
          expression.eval.asValue match {
            case v:BooleanValue => _ref = Some(v)
            case v:IntValue => _ref = Some(BooleanValue(v.value != 0))
            case v:StringValue => _ref = Some(BooleanValue(v.text.toBoolean))
            case v =>
              throw new RuntimeException(s"Can not convert value from ${v.text} to $tp")
          }
        case tp if tp == Type.int64() =>
          expression.eval.asValue match {
            case v:BooleanValue => _ref = Some(IntValue(if (v.f) 1 else 0))
            case v:IntValue => _ref = Some(v)
            case v:DoubleValue => _ref = Some(IntValue(v.value.toLong))
            case v:StringValue => _ref = Some(IntValue(java.lang.Long.parseLong(v.text)))
            case v =>
              throw new RuntimeException(s"Can not convert value from ${v.text} to $tp")
          }

        case tp if tp == Type.float64() =>
          expression.eval.asValue match {
            case v:IntValue => _ref = Some(DoubleValue(v.value.toDouble))
            case v:DoubleValue => _ref = Some(v)
            case v:StringValue => _ref = Some(DoubleValue(v.text.toDouble))
            case v =>
              throw new RuntimeException(s"Can not convert value from ${v.text} to $tp")
          }
        case tp if tp == Type.string() =>
          expression.eval.asValue match {
            case v:BooleanValue => _ref = Some(StringValue(v.text))
            case v:IntValue => _ref = Some(StringValue(v.text))
            case v:DoubleValue => _ref = Some(StringValue(v.text))
            case v:StringValue => _ref = Some(v)
            case v:DateValue => _ref = Some(StringValue(v.text))
            case v:TimestampValue => _ref = Some(StringValue(v.text))
            case v =>
              throw new RuntimeException(s"Can not convert value from ${v.text} to $tp")
          }
        case tp if tp == Type.date() =>
          expression.eval.asValue match {
            case v:DateValue => _ref = Some(v)
            case v:StringValue => _ref = Some(DateValue(Date.parseDate(v.text).toString))
            case v:TimestampValue =>
              val t = new DateTime(Timestamp.parseTimestamp(v.text).getSeconds * 1000)
              _ref = Some(DateValue(Date.fromYearMonthDay(t.getYear,t.getMonthOfYear,t.getDayOfMonth).toString))
            case v =>
              throw new RuntimeException(s"Can not convert value from ${v.text} to $tp")
          }
        case tp if tp == Type.timestamp() =>
          expression.eval.asValue match {
            case v:DateValue =>
              val d = Date.parseDate(v.text)
              _ref = Some(DateValue(Timestamp.of( new DateTime(d.getYear,d.getMonth,d.getDayOfMonth).toDate  ).toString))
            case v:StringValue => _ref = Some(TimestampValue(Timestamp.parseTimestamp(v.text).toString))
            case v:TimestampValue => _ref = Some(v)
            case v =>
              throw new RuntimeException(s"Can not convert value from ${v.text} to $tp")
          }
        case tp if tp == Type.bytes() =>
          expression.eval.asValue match {
            case v:StringValue => _ref = Some(BytesValue(v.text))
            case v:BytesValue => _ref = Some(v)
            case v =>
              throw new RuntimeException(s"Can not convert value from ${v.text} to $tp")
          }
        case _ =>
          throw new RuntimeException(s"Can not convert value to $toType")
      }
    }
    this
  }
  override def resolveReference(columns:Map[String,TableColumnValue]): Map[String,TableColumnValue] = {
    expression.resolveReference(columns)
  }

  override def invalidateEvaluatedValueIfContains(values: List[Value]): Boolean = {
    if (expression.invalidateEvaluatedValueIfContains(values)) {
      _ref = None
      true
    } else false     
  }
}

/**
  *
  * @param nat A Nat instance
  * @param subquery a subquery
  * @param qc a query context
  */
case class SubqueryValue(nat:Nat,subquery:String,qc:QueryContext,expectedColumns:Int = 0) extends Value {
  import Bolt._

  private var _result:Option[List[Struct]] = None
  private var _isArray = false
  private var _numColumns = 0
  private var _arrayType:Type = null
  private var _aliases = Map.empty[String,QueryResultAlias]

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
      case v if v == Type.array(Type.bytes()) =>
        true
      case _ =>
        false
    }

  private def _equalsAllColumnType(st:Struct):Boolean = {
    if (_numColumns > 0) {
      val t = st.getColumnType(0)
      _arrayType = t
      _isArray = _isArrayType(t)
      (1 until _numColumns).forall(st.getColumnType(_) == t)
    } else { true }
  }

  override def eval:Value = {
    if (_result.isEmpty) {
      val r = nat.executeNativeQuery(subquery).autoclose(
        _.map(_.getCurrentRowAsStruct).toList)
      r.headOption.foreach {
        st =>
          _numColumns = st.getColumnCount
      }
//      if (r.length > 1 && _numColumns > 1)
//        throw new RuntimeException(s"The subquery has multi rows:$subquery")
//
//      r.headOption.foreach(
//        row =>
//          if (!_equalsAllColumnType(row))
//            throw new RuntimeException
//      )
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
    this.eval

    val r = _result.get

    r.headOption.foreach(
      row =>
        if (!_equalsAllColumnType(row))
          throw new RuntimeException("The subquery columns are not single type")
    )

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

  override def setTo(m: Mutation.WriteBuilder, key: String): Unit = {
    this.eval.asValue.setTo(m,key)
  }

  def setAlias(aliases:Map[String,QueryResultAlias]):Unit =
    _aliases = aliases

  override def getField(fieldIdx: Int): Value = {
    this.eval
    val l = if (_numColumns == 0) expectedColumns else _numColumns
    if (fieldIdx < 0 || l <= fieldIdx)
      throw new RuntimeException("Field index is out of range")
    _result.get.headOption.map {
      row =>
        _getColumn(row, fieldIdx)
    }.getOrElse(NullValue)
  }

  override def getField(fieldName: String): Value = {
    this.eval
    val r = _result.get
    r.headOption match {
      case Some(st) =>
        try {
          _getColumn(st,st.getColumnIndex(fieldName))
        } catch {
          case _ : IllegalArgumentException =>
            throw new RuntimeException(s"Invalid column name '$fieldName'")
        }

      case None =>
        NullValue   // TODO: Check
    }
  }

  def numRows:Int = _result.map(_.length).getOrElse(0)

  def results = _result
}
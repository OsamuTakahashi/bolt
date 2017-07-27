/**
  * Bolt
  * SubqueryValue
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt.values

import com.google.cloud.spanner.{Mutation, Struct, Type}
import com.sopranoworks.bolt.Bolt.Nat
import com.sopranoworks.bolt.{Bolt, QueryContext, QueryResultAlias}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

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
/**
  * Bolt
  * IdentifierValue
  * IdentifierWithFieldValue
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt.values

import com.sopranoworks.bolt.{ExpressionAlias, QueryContext, QueryResultAlias, TableAlias}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

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
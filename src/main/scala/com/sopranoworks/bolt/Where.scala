/**
  * Bolt
  * Where
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt

import com.google.cloud.spanner.{Key, KeySet, Mutation}
import com.sopranoworks.bolt.values._

/**
  * Created by takahashi on 2017/03/28.
  */
case class Where(qc:QueryContext,var tableName:String,whereStmt:String,boolExpression:Value) {
  private var _kv:Option[List[KeyValue]] = None
  private var _tbl:Option[Table] = None

  private def _extractAndExpression(v:Value):List[Value] = {
    v() match {
      case BooleanExpressionValue("AND",a,b) =>
        _extractAndExpression(a) ++ _extractAndExpression(b)
      case _ =>
        List(v)
    }
  }

  def eval():Unit = {
    for {
      ex  <- Option(boolExpression)
      _   <- Option(qc)
      tn  <- Option(tableName).orElse(qc.currentTable)
      tbl <- qc.nut.database.table(tn)
    } {
      val k = ex.resolveReference().values
      if (k.size == tbl.primaryKey.columns.length &&
        k.forall(pk=>tbl.primaryKey.columns.exists(idx=>idx.name == pk.name))
      ) {
        k.foreach(_.setStayUnresolved(true))
        val wex = _extractAndExpression(ex.eval)
        var kv = List.empty[KeyValue]
        if (wex.length == k.size &&
          wex.forall {
            e =>
              e() match {
                case BooleanExpressionValue("=", a, b) =>
                  (a(),b.asValue) match {
                    case (t : TableColumnValue,v : LiteralValue) =>
                      kv ::= KeyValue(t.name,v)
                      true
                    case _ =>
                      false
                  }
                case _ => false
              }
          }) {
          _kv = Some(kv.reverse)
          _tbl = Some(tbl)
        }
      }
    }
  }

  def isOptimizedWhere:Boolean = _kv.isDefined

  def setPrimaryKeys(m:Mutation.WriteBuilder):Unit = {
    _kv.foreach(_.foreach(kv=>kv.value.setTo(m,kv.key)))
  }

  def asDeleteMutation:Mutation = {
      (for {
        kvs <- _kv
        tbl <- _tbl
      } yield {
        for {
          pk  <- tbl.primaryKey.columns
          kv  <- kvs.find(_.key == pk.name)
        } yield {
          kv.value.text
        }
      }).map(pks => Mutation.delete(tableName,Key.of(pks.toArray)))
        .getOrElse(throw new RuntimeException(s"Could not create delete-mutation from $whereStmt"))
  }
}
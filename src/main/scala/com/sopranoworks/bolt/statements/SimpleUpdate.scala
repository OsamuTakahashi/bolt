/**
  * Bolt
  * statements/SimpleUpdate
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt.statements

import com.google.cloud.spanner.TransactionRunner.TransactionCallable
import com.google.cloud.spanner.{Mutation, TransactionContext}
import com.sopranoworks.bolt.values.TableColumnValue
import com.sopranoworks.bolt.{Bolt, KeyValue, QueryContext, Where}

import scala.collection.JavaConversions._

case class SimpleUpdate(nut:Bolt.Nut,qc:QueryContext,tableName:String,keysAndValues:java.util.List[KeyValue],where:Where,hint:String = null) extends Update {

  private def _composeUpdateMutations(tr:TransactionContext,tableName:String,
                                      keysAndValues:java.util.List[KeyValue],
                                      where:Where,
                                      usedColumns:Map[String,TableColumnValue],
                                      hint:String):List[Mutation] = {
    val tbl = nut.database.table(tableName).get
    val cols = usedColumns.values.toList

    if (cols.isEmpty) where.eval()

    if (where.isOptimizedWhere) {
      val m = Mutation.newUpdateBuilder(tableName)
      where.setPrimaryKeys(m)
      keysAndValues.foreach {
        case KeyValue(k,v) =>
          v.eval.asValue.setTo(m,k)
      }
      List(m.build())
    } else {
      val (keys, values) = _getTargetKeysAndValues(tr, tbl, where.whereStmt, cols, hint)

      values.map {
        row =>
          keys.zip(row).foreach {
            case (k, v) =>
              k.setValue(v)
          }

          val m = Mutation.newUpdateBuilder(tableName)

          // set primary key value
          tbl.primaryKey.columns.foreach(col => keys.find(_.name == col.name).foreach {
            v =>
              v.setTo(m, v.name)
          })

          //
          keysAndValues.foreach {
            case KeyValue(k, v) =>
              v.invalidateEvaluatedValueIfContains(cols)
              v.eval.asValue.setTo(m, k)
          }
          m.build()
      }
    }
  }

  def execute():Unit = {
    nut.transactionContext match {
      case Some(tr) =>
        val usedColumns =
          keysAndValues.foldLeft(Map.empty[String,TableColumnValue]) {
            case (col,kv) =>
              kv.value.resolveReference(col)
          }
        nut.addMutations(_composeUpdateMutations(tr,tableName,keysAndValues,where,usedColumns,hint))

      case None =>
        Option(nut.dbClient).foreach(
          _ => nut.beginTransaction(_ => execute())
        )
    }
  }
}

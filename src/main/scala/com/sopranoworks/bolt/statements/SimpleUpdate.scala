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

case class SimpleUpdate(nut:Bolt.Nut,qc:QueryContext,tableName:String,keysAndValues:java.util.List[KeyValue],where:Where) extends Update {

  private def _composeUpdateMutations(tr:TransactionContext,tableName:String,
                                      keysAndValues:java.util.List[KeyValue],
                                      where:Where,
                                      usedColumns:Map[String,TableColumnValue]):List[Mutation] = {
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
      val (keys, values) = _getTargetKeysAndValues(tr, tbl, where.whereStmt, cols)

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
    val usedColumns =
      keysAndValues.foldLeft(Map.empty[String,TableColumnValue]) {
        case (col,kv) =>
          kv.value.resolveReference(col)
      }

    nut.transactionContext match {
      case Some(tr) =>
        nut.addMutations(_composeUpdateMutations(tr,tableName,keysAndValues,where,usedColumns))

      case None =>
        Option(nut.dbClient).foreach(_.readWriteTransaction()
          .run(new TransactionCallable[Unit] {
            override def run(transaction: TransactionContext):Unit = {
              val ml = _composeUpdateMutations(transaction,tableName,keysAndValues,where,usedColumns)
              if (ml.nonEmpty) transaction.buffer(ml)
            }
          }))
    }
  }
}

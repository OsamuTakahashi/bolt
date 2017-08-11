/**
  * Bolt
  * statements/UpdateSelect
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt.statements

import com.google.cloud.spanner.{Mutation, TransactionContext}
import com.google.cloud.spanner.TransactionRunner.TransactionCallable
import com.sopranoworks.bolt.values.SubqueryValue
import com.sopranoworks.bolt.{Bolt, ColumnReader, QueryContext, Where}

import scala.collection.JavaConversions._

case class UpdateSelect(nut:Bolt.Nut,qc:QueryContext,tableName:String,columns:java.util.List[String],subquery:SubqueryValue,where:Where) extends Update {
  private def _updateSelect(tableName: String, columns:java.util.List[String], subquery: SubqueryValue, where: Where, tr: TransactionContext):Unit = {
    val tbl = nut.database.table(tableName).get
    tbl.primaryKey.columns.foreach(k => if (columns.contains(k.name)) throw new RuntimeException(s"${k.name} is primary key"))

    val (keys, values) = _getTargetKeysAndValues(tr, tbl, where.whereStmt, Nil)
    val res = subquery.eval.asInstanceOf[SubqueryValue].results
    val reader = new ColumnReader {}
    val ml = res.map {
      r =>
        values.zip(r).map {
          case (keyValues, vs) =>
            if (columns.length != vs.getColumnCount)
              throw new RuntimeException("")

            val m = Mutation.newUpdateBuilder(tableName)

            keys.zip(keyValues).foreach {
              case (k, v) =>
                v.setTo(m, k.name)
            }
            var idx = 0
            columns.foreach {
              col =>
                reader.getColumn(vs, idx).setTo(m, col)
                idx += 1
            }
            m.build()
        }
    }
    ml.foreach {
      mm =>
        nut.addMutations(mm)
    }
  }

  def execute():Unit = {
    nut.transactionContext match {
      case Some(tr) =>
        _updateSelect(tableName, columns, subquery, where, tr)
      case None =>
        Option(nut.dbClient).foreach(_.readWriteTransaction()
          .run(new TransactionCallable[Unit] {
            override def run(transaction: TransactionContext): Unit = {
              _updateSelect(tableName, columns, subquery, where, transaction)
              if (nut.mutations.nonEmpty) {
                transaction.buffer(nut.mutations)
                nut.clearMutations()
              }
            }
          }))
    }
  }
}

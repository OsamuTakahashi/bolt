/**
  * Bolt
  * statements/InsertSelect
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt.statements

import com.google.cloud.spanner.Mutation
import com.sopranoworks.bolt.values.SubqueryValue
import com.sopranoworks.bolt.{Bolt, ColumnReader, QueryContext}

import scala.collection.JavaConversions._

case class InsertSelect(nut:Bolt.Nut,qc:QueryContext,tableName:String,columns:java.util.List[String],subquery:SubqueryValue) extends Insert {
  private def _createMutationsWithoutColumns = {
    val columns = _tableColumns(tableName)
    val reader = new ColumnReader {}
    val ml = subquery.eval.asInstanceOf[SubqueryValue].results.map(_.map {
      st =>
        if (st.getColumnCount != columns.length)
          throw new RuntimeException("")

        val m = Mutation.newInsertBuilder(tableName)
        columns.foreach {
          col =>
            reader.getColumn(st, col.position).asValue.setTo(m, col.name)
        }
        m.build()
    })
    ml
  }

  def insertSelectWithoutColumns():Unit =
    nut.transactionContext match {
      case Some(_) =>
        _createMutationsWithoutColumns.foreach(nut.addMutations)
      case _ =>
        Option(nut.dbClient).foreach(
          _ => nut.beginTransaction(_ =>insertSelectWithoutColumns())
        )
    }

  private def _createMutationsWithColumns = {
    val reader = new ColumnReader {}
    val ml = subquery.eval.asInstanceOf[SubqueryValue].results.map(_.map {
      st =>
        if (st.getColumnCount != columns.length)
          throw new RuntimeException("")

        val m = Mutation.newInsertBuilder(tableName)
        var idx = 0
        columns.foreach {
          col =>
            reader.getColumn(st, idx).setTo(m, col)
            idx += 1
        }
        m.build()
    })
    ml
  }

  private def insertSelectWithColumns():Unit =
    nut.transactionContext match {
      case Some(_) =>
        _createMutationsWithColumns.foreach(nut.addMutations)
      case _ =>
        Option(nut.dbClient).foreach(
          _ => nut.beginTransaction(_ => insertSelectWithColumns())
        )
    }

  override def execute(): Unit = {
    Option(columns) match {
      case Some(_) =>
        insertSelectWithColumns()
      case None =>
        insertSelectWithoutColumns()
    }
  }
}
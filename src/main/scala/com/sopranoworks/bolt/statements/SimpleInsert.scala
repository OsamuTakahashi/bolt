/**
  * Bolt
  * statements/SimpleInsert
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
import com.sopranoworks.bolt.values.Value
import com.sopranoworks.bolt.{Bolt, QueryContext}

import scala.collection.JavaConversions._

case class SimpleInsert(nut:Bolt.Nut,qc:QueryContext,tableName:String,columns:java.util.List[String],values:java.util.List[Value]) extends Insert {

  private def _createMutationsWithoutColumns = {
    val m = Mutation.newInsertBuilder(tableName)
    _tableColumns(tableName).zip(values).foreach {
      case (k, v) =>
        v.eval.asValue.setTo(m, k.name)
    }
    List(m.build())
  }

  private def _insertWithoutColumns():Unit = {
    nut.transactionContext match {
      case Some(_) =>
        val m = _createMutationsWithoutColumns
        nut.addMutations(m)
      case _ =>
        Option(nut.dbClient).foreach(
          _=> nut.beginTransaction(_ =>_insertWithoutColumns())
        )
    }
  }

  private def _createMutationsWithColumns = {
    val m = Mutation.newInsertBuilder(tableName)
    columns.zip(values).foreach {
      case (k, v) =>
        v.eval.asValue.setTo(m, k)
    }
    List(m.build())
  }

  private def _insertWithColumns():Unit = {
    nut.transactionContext match {
      case Some(_) =>
        val m = _createMutationsWithColumns
        nut.addMutations(m)
      case _ =>
        Option(nut.dbClient).foreach(
          _=> nut.beginTransaction(_ => _insertWithColumns())
        )
    }
  }

  override def execute(): Unit = {
    Option(columns) match {
      case Some(_) =>
        _insertWithColumns()
      case None =>
        _insertWithoutColumns()
    }
  }
}

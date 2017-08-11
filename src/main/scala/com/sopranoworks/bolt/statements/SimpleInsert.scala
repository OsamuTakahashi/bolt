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
  private def _insertWithOutColumns():Unit = {
    val m = Mutation.newInsertBuilder(tableName)
    _tableColumns(tableName).zip(values).foreach {
      case (k,v) =>
        v.eval.asValue.setTo(m,k.name)
    }
    nut.transactionContext match {
      case Some(_) =>
        nut.addMutations(List(m.build()))
      case _ =>
        Option(nut.dbClient).foreach(_.write(List(m.build())))
    }
  }

  private def _insertWithColumns():Unit = {
    val m = Mutation.newInsertBuilder(tableName)
    columns.zip(values).foreach {
      case (k,v) =>
        v.eval.asValue.setTo(m,k)
    }
    nut.transactionContext match {
      case Some(_) =>
        nut.addMutations(List(m.build()))
      case _ =>
        Option(nut.dbClient).foreach(_.write(List(m.build())))
    }
  }

  override def execute(): Unit = {
    Option(columns) match {
      case Some(_) =>
        _insertWithColumns()
      case None =>
        _insertWithOutColumns()
    }
  }
}

/**
  * Bolt
  * statements/BulkInsert
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

case class BulkInsert(nut:Bolt.Nut,qc:QueryContext,tableName:String,columns:java.util.List[String],values:java.util.List[java.util.List[Value]]) extends Insert {
  private def bulkInsertWithColumns():Unit = {
    val mm = values.map {
      v =>
        val m = Mutation.newInsertBuilder(tableName)
        columns.zip(v).foreach {
          case (k,vv) =>
            vv.eval.asValue.setTo(m,k)
        }
        m.build()
    }

    nut.transactionContext match {
      case Some(_) =>
        nut.addMutations(mm.toList)
      case _ =>
        Option(nut.dbClient).foreach(_.write(mm))
    }
  }

  private def bulkInsertWithOutColumns():Unit = {
    val columns = _tableColumns(tableName)
    val mm = values.map {
      v =>
        val m = Mutation.newInsertBuilder(tableName)
        columns.zip(v).foreach {
          case (k,vv) =>
            vv.eval.asValue.setTo(m,k.name)
        }
        m.build()
    }

    nut.transactionContext match {
      case Some(_) =>
        nut.addMutations(mm.toList)
      case _ =>
        Option(nut.dbClient).foreach(_.write(mm))
    }
  }

  override def execute(): Unit = {
    Option(columns) match {
      case Some(_) =>
        bulkInsertWithColumns()
      case None =>
        bulkInsertWithOutColumns()
    }
  }
}
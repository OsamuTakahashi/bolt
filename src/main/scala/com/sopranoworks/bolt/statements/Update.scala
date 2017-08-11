/**
  * Bolt
  * statements/Update
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt.statements

import com.google.cloud.spanner.{Statement, TransactionContext}
import com.sopranoworks.bolt.values.{TableColumnValue, Value}
import com.sopranoworks.bolt._

trait Update {
  import Bolt._

  def nut:Bolt.Nut

  protected def _getTargetKeys(transaction: TransactionContext,tableName:String,where:String):List[List[String]] = {
    val tbl = nut.database.table(tableName).get
    val keyNames = tbl.primaryKey.columns.map(_.name)
    val resSet = transaction.executeQuery(Statement.of(s"SELECT ${keyNames.mkString(",")} FROM $tableName $where"))
    val reader = new ColumnReader {}

    resSet.autoclose {
      res =>
        res.map {
          row =>
            (0 until row.getColumnCount).map(i=>reader.getColumn(row,i).text).toList
        }.toList
    }
  }

  protected def _getTargetKeysAndValues(transaction: TransactionContext,
                                      tbl:Table,
                                      where:String,
                                      usedColumns:List[TableColumnValue]):(List[TableColumnValue],List[List[Value]]) = {
    val keyNames = tbl.primaryKey.columns.filter(col=> !usedColumns.exists(_.name == col.name)).map(col => TableColumnValue(col.name,tbl.name,col.position)) ++ usedColumns
    val resSet = transaction.executeQuery(Statement.of(s"SELECT ${keyNames.map(_.name).mkString(",")} FROM ${tbl.name} $where"))
    val reader = new ColumnReader {}

    (keyNames,
      resSet.autoclose {
        res =>
          res.map {
            row =>
              (0 until row.getColumnCount).map(i=>reader.getColumn(row,i)).toList
          }.toList
      })
  }
  
  def execute():Unit
}
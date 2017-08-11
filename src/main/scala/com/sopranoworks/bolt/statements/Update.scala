package com.sopranoworks.bolt.statements

import com.google.cloud.spanner.TransactionRunner.TransactionCallable
import com.google.cloud.spanner.{Mutation, Statement, TransactionContext}
import com.sopranoworks.bolt.values.{SubqueryValue, TableColumnValue, Value}
import com.sopranoworks.bolt._

import scala.collection.JavaConversions._

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

case class SimpleUpdate(nut:Bolt.Nut,qc:QueryContext,tableName:String,keysAndValues:java.util.List[KeyValue],where:Where) extends Update {
  import Bolt._

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

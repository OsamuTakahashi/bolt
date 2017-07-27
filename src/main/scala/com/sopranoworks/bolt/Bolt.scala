/**
  * Bolt
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt

import java.io.ByteArrayInputStream

import com.sopranoworks.bolt.values._
import com.google.cloud.Date
import com.google.cloud.spanner.TransactionRunner.TransactionCallable
import com.google.cloud.spanner.{DatabaseClient, Key, KeySet, Mutation, ResultSet, ResultSets, Statement, Struct, TransactionContext, Type, Value => SValue}
//import com.google.cloud.spanner._
import org.antlr.v4.runtime._
import org.slf4j.LoggerFactory

import scala.collection.AbstractIterator
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Created by takahashi on 2017/03/28.
  */
object Bolt {
  private val _logger = LoggerFactory.getLogger(this.getClass)

  /**
    * A Google Cloud Spanner java client library wrapper class
    * @note This class is NOT THREAD SAFE!
    * @param dbClient A database clinet
    */
  implicit class Nat(val dbClient: DatabaseClient) {
    private var _transactionContext: Option[TransactionContext] = None
    private var _mutations = List.empty[Mutation]

    def transactionContext = _transactionContext

    def database = Database(dbClient)

    /**
      * Executing INSERT/UPDATE/DELETE query on Google Cloud Spanner
      * @param srcSql sql statements
      */
    def executeQuery(srcSql:String,admin:Admin,instanceId:String): ResultSet = {
      val source = new ByteArrayInputStream(srcSql.getBytes("UTF8"))
      val input = new ANTLRInputStream(source)
      val lexer = new MiniSqlLexer(input)
//        lexer.removeErrorListeners()

      val tokenStream = new CommonTokenStream(lexer)
      val parser = new MiniSqlParser(tokenStream)
      parser.setErrorHandler(new BailErrorStrategy())
      parser.nat = this
      parser.admin = admin
      parser.instanceId = instanceId
//        parser.removeErrorListeners()

      try {
        parser.minisql().resultSet
      } catch {
        case e : Exception =>
          if (parser.minisql().resultSet != null)
            parser.minisql().resultSet.close()
          throw e
      }
    }

    def executeQuery(sql:String): ResultSet = executeQuery(sql,null,null)

    /**
      * Internal use
      */
    def isKey(tableName:String,columnName:String):Boolean =
      Database(dbClient).table(tableName).get.primaryKey.equalKeys(List(columnName)) // !! TEMPORARY !!

    private def _tableColumns(tableName:String) =
      database.table(tableName) match {
        case Some(tbl) =>
          tbl.columns
        case None =>
          throw new RuntimeException(s"Table not found $tableName")
      }

    /**
      * Internal use
      */
    def insert(tableName:String,values:java.util.List[Value]):Unit = {
      val m = Mutation.newInsertBuilder(tableName)
      _tableColumns(tableName).zip(values).foreach {
        case (k,v) =>
          v.setTo(m,k.name)
      }
      _transactionContext match {
        case Some(_) =>
          _mutations ++= List(m.build())
        case _ =>
          Option(dbClient).foreach(_.write(List(m.build())))
      }
    }

    def insert(tableName:String,columns:java.util.List[String],values:java.util.List[Value]):Unit = {
      val m = Mutation.newInsertBuilder(tableName)
      columns.zip(values).foreach {
        case (k,v) =>
          v.setTo(m,k)
      }
      _transactionContext match {
        case Some(_) =>
          _mutations ++= List(m.build())
        case _ =>
          Option(dbClient).foreach(_.write(List(m.build())))
      }
    }

    def bulkInsert(tableName:String,columns:java.util.List[String],values:java.util.List[java.util.List[Value]]):Unit = {
      val mm = values.map {
        v =>
          val m = Mutation.newInsertBuilder(tableName)
          columns.zip(v).foreach {
            case (k,vv) =>
              vv.setTo(m,k)
          }
          m.build()
      }

      _transactionContext match {
        case Some(_) =>
          _mutations ++= mm
        case _ =>
          Option(dbClient).foreach(_.write(mm))
      }
    }

    def bulkInsert(tableName:String,values:java.util.List[java.util.List[Value]]):Unit = {
      val m = Mutation.newInsertBuilder(tableName)
      val columns = _tableColumns(tableName)
      val mm = values.map {
        v =>
          val m = Mutation.newInsertBuilder(tableName)
          columns.zip(v).foreach {
            case (k,vv) =>
              vv.setTo(m,k.name)
          }
          m.build()
      }

      _transactionContext match {
        case Some(_) =>
          _mutations ++= mm
        case _ =>
          Option(dbClient).foreach(_.write(mm))
      }
    }

    def insertSelect(tableName:String,subquery:SubqueryValue):Unit = {
      val columns = _tableColumns(tableName)
      val reader = new ColumnReader {}
      val ml = subquery.eval.asInstanceOf[SubqueryValue].results.map(_.map {
        st =>
          if (st.getColumnCount != columns.length)
            throw new RuntimeException("")

          val m = Mutation.newInsertBuilder(tableName)
          columns.foreach {
            col =>
              reader.getColumn(st,col.position).setTo(m,col.name)
          }
          m.build()
      })
      ml.foreach {
        mm =>
          _transactionContext match {
            case Some(_) =>
              _mutations ++= mm
            case _ =>
              Option(dbClient).foreach(_.write(mm))
          }
      }
    }

    def insertSelect(tableName:String,columns:java.util.List[String],subquery:SubqueryValue):Unit = {
      val reader = new ColumnReader {}
      val ml = subquery.eval.asInstanceOf[SubqueryValue].results.map(_.map {
        st =>
          if (st.getColumnCount != columns.length)
            throw new RuntimeException("")

          val m = Mutation.newInsertBuilder(tableName)
          var idx = 0
          columns.foreach {
            col =>
              reader.getColumn(st,idx).setTo(m,col)
              idx += 1
          }
          m.build()
      })
      ml.foreach {
        mm =>
          _transactionContext match {
            case Some(_) =>
              _mutations ++= mm
            case _ =>
              Option(dbClient).foreach(_.write(mm))
          }
      }
    }
    def executeNativeQuery(sql:String):ResultSet =
      _transactionContext match {
        case Some(tr) =>
          tr.executeQuery(Statement.of(sql))
        case None =>
          dbClient.singleUse().executeQuery(Statement.of(sql))
      }

    def executeNativeAdminQuery(admin:Admin,sql:String):Unit = {
      Option(admin) match {
        case Some(a) =>
          val op = a.adminClient.updateDatabaseDdl(a.instance,a.databaes,List(sql),null)
          op.waitFor().getResult
          Database.reloadWith(dbClient)
        case None =>
          _logger.warn("Could not execute administrator query")
      }
    }

    private def _getTargetKeys(transaction: TransactionContext,tableName:String,where:String):List[List[String]] = {
      val tbl = database.table(tableName).get
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

    private def _getTargetKeysAndValues(transaction: TransactionContext,
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

//    private def _preUpdateQuery(usedColumns:Map[String,TableColumnValue]):ResultSet = {
//
//    }

    private def _composeUpdateMutations(tr:TransactionContext,tableName:String,
                                        keysAndValues:java.util.List[KeyValue],
                                        where:NormalWhere,
                                        usedColumns:Map[String,TableColumnValue]):List[Mutation] = {
      val tbl = database.table(tableName).get
      val cols = usedColumns.values.toList
      val (keys,values) = _getTargetKeysAndValues(tr,tbl,where.whereStmt,cols)

      values.map {
        row =>
          keys.zip(row).foreach {
            case (k,v) =>
              k.setValue(v)
          }

          val m = Mutation.newUpdateBuilder(tableName)

          // set primary key value
          tbl.primaryKey.columns.foreach(col => keys.find(_.name == col.name).foreach {
            v =>
              v.setTo(m,v.name)
          })

          //
          keysAndValues.foreach {
            case KeyValue(k,v) =>
              v.invalidateEvaluatedValueIfContains(cols)
              v.eval.asValue.setTo(m,k)
          }
          m.build()
      }
    }

    /**
      * Internal use
      */
    def update(tableName:String,keysAndValues:java.util.List[KeyValue],where:NormalWhere):Unit = {
      val usedColumns =
        keysAndValues.foldLeft(Map.empty[String,TableColumnValue]) {
          case (col,kv) =>
            kv.value.resolveReference(col)
        }

//      if (usedColumns.nonEmpty) {
//        // required pre query
//      }

      _transactionContext match {
        case Some(tr) =>
          _mutations ++= _composeUpdateMutations(tr,tableName,keysAndValues,where,usedColumns)

        case None =>
          Option(dbClient).foreach(_.readWriteTransaction()
            .run(new TransactionCallable[Unit] {
              override def run(transaction: TransactionContext):Unit = {
                val ml = _composeUpdateMutations(transaction,tableName,keysAndValues,where,usedColumns)
                if (ml.nonEmpty) transaction.buffer(ml)
              }
            }))
      }
    }

    private def _updateSelect(tableName: String, columns:java.util.List[String], subquery: SubqueryValue, where: NormalWhere, tr: TransactionContext) = {
      val tbl = database.table(tableName).get
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
          _mutations ++= mm
      }
    }

    def update(tableName:String,columns:java.util.List[String],subquery:SubqueryValue,where:NormalWhere):Unit = {
      _transactionContext match {
        case Some(tr) =>
          _updateSelect(tableName, columns, subquery, where, tr)
        case None =>
          Option(dbClient).foreach(_.readWriteTransaction()
            .run(new TransactionCallable[Unit] {
              override def run(transaction: TransactionContext): Unit = {
                _updateSelect(tableName, columns, subquery, where, transaction)
                if (_mutations.nonEmpty) {
                  transaction.buffer(_mutations)
                  _mutations = List.empty[Mutation]
                }
              }
            }))
      }
    }

    /**
      * Internal use
      */
    def delete(tableName:String,where:Where):Unit = {
      Option(where) match {
        case Some(PrimaryKeyWhere(k,v)) =>
          val m = Mutation.delete(tableName,Key.of(v))
          _transactionContext match {
            case Some(_) =>
              _mutations ++= List(m)
            case _ =>
              Option(dbClient).foreach(_.write(List(m)))
          }

        case Some(NormalWhere(w,_)) =>
//          _logger.info(s"Slow delete query on $tableName. Reason => $w")
          _transactionContext match {
            case Some(tr) =>
              val keys = _getTargetKeys(tr,tableName,w)
              if (keys.nonEmpty) {
                val ml = keys.map {
                  k =>
                    Mutation.delete(tableName,Key.of(k:_*))
                }
                _mutations ++= ml
              }

            case _ =>
              Option(dbClient).foreach(_.readWriteTransaction()
                .run(new TransactionCallable[Unit] {
                  override def run(transaction: TransactionContext):Unit = {
                    val keys = _getTargetKeys(transaction,tableName,w)
                    if (keys.nonEmpty) {
                      val ml = keys.map {
                        k =>
                          Mutation.delete(tableName,Key.of(k:_*))
                      }
                      transaction.buffer(ml)
                    }
                  }
                }))
          }

        case None =>
          val m = Mutation.delete(tableName,KeySet.all())
          Option(dbClient).foreach(_.write(List(m)))
        case _ =>
      }
    }

    def createDatabase(admin:Admin,instanceId:String,databaseName:String):Boolean = {
      (Option(admin),Option(instanceId)) match {
        case (Some(admin),Some(iid)) =>
          val r = admin.adminClient.createDatabase(iid,databaseName,List.empty[String].asJava).waitFor()
          r.isSuccessful
        case _ =>
          false
      }
    }

    def showIndexes(tableName:String):ResultSet = {
      dbClient.singleUse().executeQuery(Statement.of(s"SELECT INDEX_NAME,INDEX_TYPE,IS_UNIQUE,IS_NULL_FILTERED FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_NAME='$tableName'"))
    }

    def showTables():ResultSet = {
      dbClient.singleUse().executeQuery(Statement.of("SELECT TABLE_NAME,PARENT_TABLE_NAME,ON_DELETE_ACTION FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=\"\""))
    }

    def showColumns(table:String):ResultSet =
      dbClient.singleUse().executeQuery(Statement.of(s"SELECT COLUMN_NAME,SPANNER_TYPE,IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='$table' ORDER BY ORDINAL_POSITION"))


    def showDatabases(admin:Admin,instanceId:String):ResultSet = {
      (Option(admin),Option(instanceId)) match {
        case (Some(adm),Some(iid)) =>
          val p = adm.adminClient.listDatabases(iid)
          val dbs = p.iterateAll().map {
            db =>
              val b= Struct.newBuilder()
              b.add("DATABASE_NAME",SValue.string(db.getId.getDatabase)).build()
          }
          ResultSets.forRows(Type.struct(List(Type.StructField.of("DATABASE_NAME",Type.string()))),dbs.asJava)

        case _ =>
          null
      }
    }

    private def _primaryKeyForTable(tableName:String):Option[String] = {
      val key = dbClient.singleUse().executeQuery(Statement.of(s"SELECT COLUMN_NAME,COLUMN_ORDERING FROM INFORMATION_SCHEMA.INDEX_COLUMNS WHERE TABLE_NAME='$tableName' AND INDEX_NAME='PRIMARY_KEY' ORDER BY ORDINAL_POSITION")).map {
        col =>
          s"${col.getString(0)} ${if (col.isNull(1)) "" else col.getString(1)}"
      }.mkString(",")
      if (key.isEmpty) None else Some(key)
    }

    private def _deleteActionForTable(tableName:String):Option[String] = {
      val r = dbClient.singleUse().executeQuery(Statement.of(s"SELECT PARENT_TABLE_NAME,ON_DELETE_ACTION FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='' AND TABLE_NAME='$tableName'")).map {
        row =>
          s"${if (row.isNull(0)) "" else s"INTERLEAVE IN PARENT ${row.getString(0)}"} ${if (row.isNull(1)) "" else s"ON DELETE ${row.getString(1)}"}"
      }
      r.toList.headOption
    }

    private def _addComma(columns:List[String]):List[String] = {
      columns.reverse match {
        case head :: tail => (head :: tail.map(_ + ",")).reverse
        case head :: Nil => columns
      }
    }

    private def _showCreateIndexOnTable(tableName:String):List[String] = {
      dbClient.singleUse()
        .executeQuery(Statement.of(s"SELECT INDEX_NAME,IS_UNIQUE,IS_NULL_FILTERED,PARENT_TABLE_NAME FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_NAME='$tableName' AND INDEX_TYPE='INDEX'"))
        .autoclose(_.map(r=>(r.getString(0),r.getBoolean(1),r.getBoolean(2),if (!r.isNull(3)) Some(r.getString(3)) else None)).toList).flatMap {
        case (idx,uniq,nullFiltered,parent) =>
          s"CREATE ${if (uniq) "UNIQUE " else ""}${if (nullFiltered) "NULL_FILTERED " else ""}INDEX IF NOT EXISTS $idx ON $tableName (" ::
          dbClient.singleUse()
            .executeQuery(Statement.of(s"SELECT COLUMN_NAME,COLUMN_ORDERING FROM INFORMATION_SCHEMA.INDEX_COLUMNS WHERE TABLE_NAME='$tableName' AND INDEX_NAME='$idx' AND COLUMN_ORDERING IS NOT NULL ORDER BY ORDINAL_POSITION"))
            .autoclose(_.map(r=>s"${r.getString(0)} ${r.getString(1)}").toList.mkString(",")) ::
          ")" + {
            val e = dbClient.singleUse()
            .executeQuery(Statement.of(s"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.INDEX_COLUMNS WHERE TABLE_NAME='$tableName' AND INDEX_NAME='$idx' AND COLUMN_ORDERING IS NULL"))
              .autoclose(_.map(_.getString(0)).toList)
              if (e.nonEmpty) e.mkString("STORING (",",",")") else ""
          } ::
          parent.map(p => if (p.isEmpty) p else s", INTERLEAVE IN $p").getOrElse("") + ";" :: Nil
      }
    }

    def showCreateTable(tableName:String):ResultSet = {
      val tbl = (List(s"CREATE TABLE IF NOT EXISTS $tableName (") ++
        _addComma(showColumns(tableName).map {
          col =>
            s"  ${col.getString("COLUMN_NAME")} ${col.getString("SPANNER_TYPE")} ${if (col.getString("IS_NULLABLE").toUpperCase.startsWith("T")) "" else "NOT NULL"}"
        }.toList) ++
        List(s") ${_primaryKeyForTable(tableName).map(k => s"PRIMARY KEY ($k)").getOrElse("") }", s"${ _deleteActionForTable(tableName).getOrElse(""); };") ++
        _showCreateIndexOnTable(tableName))
          .map(s=>Struct.newBuilder().add(tableName,SValue.string(s)).build())
      ResultSets.forRows(Type.struct(List(Type.StructField.of(tableName,Type.string()))),tbl)
    }

    def tableExists(tableName:String):Boolean =
      dbClient.singleUse().executeQuery(Statement.of(s"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='' AND TABLE_NAME='$tableName'")).autoclose(_.map(_ =>true).toList.nonEmpty)

    def indexExists(tableName:String,indexName:String):Boolean =
      dbClient.singleUse().executeQuery(Statement.of(s"SELECT INDEX_NAME FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_NAME='$tableName' AND INDEX_NAME='$indexName'")).autoclose(_.map(_ =>true).toList.nonEmpty)

    /**
      * Internal use
      * To resolve Antlr4's 'throws' keyword issue
      * Currently, when 'throws' is used in the grammar, it will cause strange result on compile time.
      * TODO: Modify to use 'throws' when it is resolved.
      */
    def changeDatabase(name:String):Unit =
      throw DatabaseChangedException(name)


    def beginTransaction[T](f:Nat => T):T = {
      val self = this
      dbClient.readWriteTransaction()
        .run(new TransactionCallable[T] {
          override def run(transaction: TransactionContext): T = {
            _transactionContext = Some(transaction)
            val r = f(self)
            if (_mutations.nonEmpty) {
              transaction.buffer(_mutations)
            }
            _transactionContext = None
            _mutations = List.empty[Mutation]
            r
          }
        })
    }

    def beginTransaction[T](f:Nat => T,default:T):T = {
      val self = this
      dbClient.readWriteTransaction()
        .run(new TransactionCallable[T] {
          override def run(transaction: TransactionContext): T = {
            _transactionContext = Some(transaction)
            val r = f(self)
            if (_mutations.nonEmpty) {
              transaction.buffer(_mutations)
            }
            _transactionContext = None
            _mutations = List.empty[Mutation]
            r
          }
        })
    }

    /**
      * For java
      */
    def beginTransaction[T](t:Transaction[T]):T = {
      val self = this
      dbClient.readWriteTransaction()
        .run(new TransactionCallable[T] {
          override def run(transaction: TransactionContext): T = {
            _transactionContext = Some(transaction)
            val r = t.run(self)
            if (_mutations.nonEmpty) {
              transaction.buffer(_mutations)
            }
            _transactionContext = None
            _mutations = List.empty[Mutation]
            r
          }
        })
    }

    def rollback():Unit = _transactionContext.foreach(_ => _mutations = List.empty[Mutation])
  }

  trait Transaction[T] {
    def run(nat:Nat):T
  }


  implicit def resultSetToIterator(resultSet: ResultSet):Iterator[ResultSet] = new AbstractIterator[ResultSet] {
    override def hasNext: Boolean =
      resultSet.next()
    override def next(): ResultSet =
      resultSet
  }

  implicit class Washer(resultSet: ResultSet) {
    def iterator = resultSetToIterator(resultSet)
    def headOption:Option[ResultSet] =
      if (resultSet.next()) Some(resultSet) else None
  }

  implicit class Sealer(resultSet: ResultSet) {
    def autoclose[T](f: ResultSet => T):T = try {
      f(resultSet)
    } finally {
      resultSet.close()
    }
  }

  implicit class SafeResultSet(resultSet: ResultSet) {
    def getBooleanOpt(name:String):Option[Boolean] =
      try {
        Some(resultSet.getBoolean(name))
      } catch {
        case _ : Exception =>
          None
      }
    def getLongOpt(name:String):Option[Long] =
      try {
        Some(resultSet.getLong(name))
      } catch {
        case _ : Exception =>
          None
      }
    def getDoubleOpt(name:String):Option[Double] =
      try {
        Some(resultSet.getDouble(name))
      } catch {
        case _ : Exception =>
          None
      }
    def getStringOpt(name:String):Option[String] =
      try {
        Some(resultSet.getString(name))
      } catch {
        case _ : Exception =>
          None
      }
    def getDateOpt(name:String):Option[Date] =
      try {
        Some(resultSet.getDate(name))
      } catch {
        case _ : Exception =>
          None
      }
  }
}

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

import com.google.cloud.Date
import com.google.cloud.spanner.TransactionRunner.TransactionCallable
import com.google.cloud.spanner.{AbortedException, DatabaseClient, Mutation, ResultSet, ResultSets, Statement, Struct, TransactionContext, Type, Value => SValue}
import com.sopranoworks.bolt.statements.NoResult
import org.antlr.v4.runtime._
import org.slf4j.LoggerFactory

import scala.collection.AbstractIterator
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.language.implicitConversions

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
  implicit class Nut(val dbClient: DatabaseClient) {
    private var _transactionContext: Option[TransactionContext] = None
    private var _mutations = List.empty[Mutation]
    private var _outerTransction = false

    def transactionContext: Option[TransactionContext] = _transactionContext
    private [bolt] def setTransactionContext(tr:TransactionContext):Unit = {
      _transactionContext = Some(tr)
      _outerTransction = true
    }

    def database = Database(dbClient)

    private [bolt] def addMutations(ms:List[Mutation]):Unit = {
      if (_outerTransction)
        _transactionContext.foreach(_.buffer(ms))
      else
        _mutations ++= ms
    }
    private [bolt] def mutations = _mutations
    private [bolt] def clearMutations():Unit = _mutations = List.empty[Mutation]

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
      parser.nut = this
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

    @inline
    def executeQuery(sql:String): ResultSet = executeQuery(sql,null,null)
    
    /**
      * An executing native spanner query method
      * @param sql SELECT query
      * @return The result of the query
      */
    private [bolt] def executeNativeQuery(sql:String):ResultSet =
      _transactionContext match {
        case Some(tr) =>
          tr.executeQuery(Statement.of(sql))
        case None =>
          dbClient.singleUse().executeQuery(Statement.of(sql))
      }

    /**
      * An execiting native admin query method
      * @param admin an admin instance
      * @param sql CREATE/DROP/ALTER query
      */
    private [bolt] def executeNativeAdminQuery(admin:Admin,sql:String):Unit = {
      Option(admin) match {
        case Some(a) =>
          val op = a.adminClient.updateDatabaseDdl(a.instance,a.databaes,List(sql),null)
          try {
            op.get()
            Database.reloadWith(dbClient)
          } catch {
            case e : Exception =>
              _logger.error(s"Executing administrator query failure with:${e.getMessage}")
          }
        case None =>
          _logger.warn("Could not execute administrator query")
      }
    }

    private [bolt] def execute(stmt:NoResult):Unit = {
      stmt.execute()
    }

    // Utility methods (for client application)

    def createDatabase(admin:Admin,instanceId:String,databaseName:String):Boolean = {
      (Option(admin),Option(instanceId)) match {
        case (Some(adm),Some(iid)) =>
          val r = adm.adminClient.createDatabase(iid,databaseName,List.empty[String].asJava)
          try {
            r.get()
            true
          } catch {
            case e : Exception =>
              _logger.error(s"Executing administrator query failure with:${e.getMessage}")
              false
          }
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

    def showSequences():ResultSet =
      dbClient.singleUse().executeQuery(Statement.of(s"SELECT NAME,DATA_TYPE FROM INFORMATION_SCHEMA.SEQUENCES"))

    def showDatabases(admin:Admin,instanceId:String):ResultSet = {
      (Option(admin),Option(instanceId)) match {
        case (Some(adm),Some(iid)) =>
          val p = adm.adminClient.listDatabases(iid)
          val dbs = p.iterateAll().map {
            db =>
              val b= Struct.newBuilder()
              b.set("DATABASE_NAME").to(SValue.string(db.getId.getDatabase)).build()
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
          s"${if (row.isNull(0)) "" else s", INTERLEAVE IN PARENT ${row.getString(0)}"} ${if (row.isNull(1)) "" else s"ON DELETE ${row.getString(1)}"}"
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
            s"  ${col.getString("COLUMN_NAME")} ${col.getString("SPANNER_TYPE")} ${if (col.getString("IS_NULLABLE").toUpperCase.startsWith("Y")) "" else "NOT NULL"}"
        }.toList) ++
        List(s") ${_primaryKeyForTable(tableName).map(k => s"PRIMARY KEY ($k)").getOrElse("") }", s"${ _deleteActionForTable(tableName).getOrElse(""); };") ++
        _showCreateIndexOnTable(tableName))
          .map(s=>Struct.newBuilder().set(tableName).to(SValue.string(s)).build())
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
    private [bolt] def changeDatabase(name:String):Unit =
      throw DatabaseChangedException(name)


    /**
      * A transaction wrapper
      *
      * @param f The function object for the operation
      * @tparam T The result type
      * @return Some result of type T
      */
    def beginTransaction[T](f:Nut => T):T = {
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
      * A transcation wrapper with default result value
      *
      * @param f The function object for the operation
      * @param default the default value on failure
      * @tparam T The result type
      * @return Some result of type T
      */
    def beginTransaction[T](f:Nut => T, default:T):T = {
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
    def run(nat:Nut):T
  }

  // Danger to use ?
//  implicit def transactionContextToNut(tr:TransactionContext):Nut = {
//    val nut = new Nut(null)
//    nut.setTransactionContext(tr)
//    nut
//  }

  implicit def dbClientAndTransactionContextToNut(pair:(DatabaseClient,TransactionContext)):Nut = {
    val nut = new Nut(pair._1)
    nut.setTransactionContext(pair._2)
    nut
  }

  implicit def resultSetToIterator(resultSet: ResultSet):Iterator[ResultSet] = new AbstractIterator[ResultSet] {
    override def hasNext: Boolean =
      resultSet.next()
    override def next(): ResultSet =
      resultSet
  }

  implicit class Washer(resultSet: ResultSet) {
    def iterator:Iterator[ResultSet] = resultSetToIterator(resultSet)
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

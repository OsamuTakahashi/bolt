package com.sopranoworks.bolt

import java.io.ByteArrayInputStream

import com.google.cloud.spanner.TransactionRunner.TransactionCallable
import com.google.cloud.spanner._
import org.antlr.v4.runtime._

import scala.collection.JavaConversions._

/**
  * Created by takahashi on 2017/03/28.
  */
object Bolt {

  /**
    * A Google Cloud Spanner java client library wrapper class
    * @param dbClient A database clinet
    */
  implicit class Nat(dbClient: DatabaseClient) {
    private implicit def _dbClient = dbClient
    private var _transactionContext: Option[TransactionContext] = None
    private var _mutations = List.empty[Mutation]

    /**
      * Executing INSERT/UPDATE/DELETE query on Google Cloud Spanner
      * @param sql a INSERT/UPDATE/DELETE sql statement
      */
    def executeQuery(sql:String): ResultSet = {
      val source = new ByteArrayInputStream( sql.getBytes("UTF8"))
      val input = new ANTLRInputStream(source)
      val lexer = new MiniSqlLexer(input)
      lexer.removeErrorListeners()

      val tokenStream = new CommonTokenStream(lexer)
      val parser = new MiniSqlParser(tokenStream)
      parser.nat = this
      parser.removeErrorListeners()

      try {
        parser.minisql().resultSet
      } catch {
        case _ : NativeSqlException =>
          _transactionContext match {
            case Some(tr) =>
              tr.executeQuery(Statement.of(sql))
            case _ =>
              dbClient.singleUse ().executeQuery (Statement.of (sql) )
          }
      }
    }

    /**
      * Internal use
      */
    def isKey(tableName:String,columnName:String):Boolean =
      Database(dbClient).table(tableName).isKey(columnName)

    /**
      * Internal use
      */
    def insert(tableName:String,values:java.util.List[String]):Unit = {
      val m = Mutation.newInsertBuilder(tableName)
      val columns = Database(dbClient).table(tableName).columns
      columns.zip(values).foreach(kv=>m.set(kv._1).to(kv._2))

      _transactionContext match {
        case Some(_) =>
          _mutations ++= List(m.build())
        case _ =>
          dbClient.write(List(m.build()))
      }
    }

    def insert(tableName:String,columns:java.util.List[String],values:java.util.List[String]):Unit = {
      val m = Mutation.newInsertBuilder(tableName)
      columns.zip(values).foreach(kv=>m.set(kv._1).to(kv._2))
      _transactionContext match {
        case Some(_) =>
          _mutations ++= List(m.build())
        case _ =>
          dbClient.write(List(m.build()))
      }
    }

    private def _getTargetKeys(transaction: TransactionContext,tableName:String,where:String):List[String] = {
      val tbl = Database(dbClient).table(tableName)
      val key = tbl.key
      val resSet = transaction.executeQuery(Statement.of(s"SELECT $key FROM $tableName $where"))
      var keys = List.empty[String]

      tbl.columnTypeOf(key) match {
        case Some("INT64") =>
          while(resSet.next()) {
            keys ::= resSet.getLong(key).toString
          }
        case Some("STRING") =>
          while(resSet.next()) {
            keys ::= resSet.getString(key)
          }
        case Some("FLOAT64") =>
          while(resSet.next()) {
            keys ::= resSet.getDouble(key).toString
          }
        case _ =>
      }
      keys.reverse
    }
    
    /**
      * Internal use
      */
    def update(tableName:String,keysAndValues:java.util.List[KeyValue],where:Where):Unit = {
      where match {
        case PrimaryKeyWhere(k,v) =>
          val m = Mutation.newUpdateBuilder(tableName)
          m.set(k).to(v)
          keysAndValues.foreach(kv=>m.set(kv.key).to(kv.value))

          _transactionContext match {
            case Some(_) =>
              _mutations ++= List(m.build())
            case _ =>
              dbClient.write(List(m.build()))
          }

        case NormalWhere(w) =>
          _transactionContext match {
            case Some(tr) =>
              val keys = _getTargetKeys(tr,tableName,w)
              if (keys.nonEmpty) {
                val key = Database(dbClient).table(tableName).key
                val ml = keys.map {
                  k =>
                    val m = Mutation.newUpdateBuilder(tableName)
                    m.set(key).to(k)
                    keysAndValues.foreach(kv=>m.set(kv.key).to(kv.value))

                    m.build()
                }
                _mutations ++= ml
              }

            case _ =>
              dbClient.readWriteTransaction()
                .run(new TransactionCallable[Unit] {
                  override def run(transaction: TransactionContext):Unit = {
                    val keys = _getTargetKeys(transaction,tableName,w)
                    if (keys.nonEmpty) {
                      val key = Database(dbClient).table(tableName).key
                      val ml = keys.map {
                        k =>
                          val m = Mutation.newUpdateBuilder(tableName)
                          m.set(key).to(k)
                          keysAndValues.foreach(kv=>m.set(kv.key).to(kv.value))

                          m.build()
                      }
                      transaction.buffer(ml)
                    }
                  }
                })
          }
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
              dbClient.write(List(m))
          }

        case Some(NormalWhere(w)) =>
          _transactionContext match {
            case Some(tr) =>
              val keys = _getTargetKeys(tr,tableName,w)
              if (keys.nonEmpty) {
                val ml = keys.map {
                  k =>
                    Mutation.delete(tableName,Key.of(k))
                }
                _mutations ++= ml
              }

            case _ =>
              dbClient.readWriteTransaction()
                .run(new TransactionCallable[Unit] {
                  override def run(transaction: TransactionContext):Unit = {
                    val keys = _getTargetKeys(transaction,tableName,w)
                    if (keys.nonEmpty) {
                      val ml = keys.map {
                        k =>
                          Mutation.delete(tableName,Key.of(k))
                      }
                      transaction.buffer(ml)
                    }
                  }
                })
          }

        case None =>
          val m = Mutation.delete(tableName,KeySet.all())
          dbClient.write(List(m))
        case _ =>
      }
    }

    /**
      * Internal use
      * To resolve Antlr4's 'throws' keyword issue
      * Currently, when 'throws' is used in the grammar, it will cause strange result on compile time.
      * TODO: Modify to use 'throws' when it is resolved.
      */
    def useNative():Unit =
      throw new NativeSqlException


    def beginTransaction(f:Nat => Unit):Unit = {
      val self = this
      dbClient.readWriteTransaction()
        .run(new TransactionCallable[Unit] {
          override def run(transaction: TransactionContext): Unit = {
            _transactionContext = Some(transaction)
            f(self)
            if (_mutations.nonEmpty) {
              transaction.buffer(_mutations)
            }
            _transactionContext = None
            _mutations = List.empty[Mutation]
          }
        })
    }

    def sql(q:String) = executeQuery(q)

    def rollback:Unit = _transactionContext.foreach(_ => _mutations = List.empty[Mutation])
  }

  implicit def resultSetToIterator(resultSet: ResultSet):Iterator[ResultSet] = Iterator.continually(resultSet).takeWhile(_.next())

  def executeQuery(sql:String)(implicit dbClient:Nat):ResultSet =
    dbClient.executeQuery(sql)
}

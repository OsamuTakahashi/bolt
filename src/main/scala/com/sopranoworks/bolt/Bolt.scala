package com.sopranoworks.bolt

import java.io.ByteArrayInputStream

import com.google.cloud.Date
import com.google.cloud.spanner.TransactionRunner.TransactionCallable
import com.google.cloud.spanner._
import org.antlr.v4.runtime._
import org.slf4j.LoggerFactory
import shapeless.HNil

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

    /**
      * Executing INSERT/UPDATE/DELETE query on Google Cloud Spanner
      * @param srcSql a INSERT/UPDATE/DELETE sql statement
      */
    def executeQuery(srcSql:String,admin:Admin): ResultSet = srcSql.split(";").map(_.trim).filter(!_.isEmpty).map {
      sql =>
        val source = new ByteArrayInputStream(sql.getBytes("UTF8"))
        val input = new ANTLRInputStream(source)
        val lexer = new MiniSqlLexer(input)
//        lexer.removeErrorListeners()

        val tokenStream = new CommonTokenStream(lexer)
        val parser = new MiniSqlParser(tokenStream)
        parser.nat = this
        parser.admin = admin
//        parser.removeErrorListeners()

        try {
          try {
            parser.minisql().resultSet
          } catch {
            case _: NativeSqlException =>
              _logger.debug("native query")
              _transactionContext match {
                case Some(tr) =>
                  tr.executeQuery(Statement.of(sql))
                case _ =>
                  dbClient.singleUse().executeQuery(Statement.of(sql))
              }
            case _: NativeAdminSqlException =>
              Option(admin) match {
                case Some(a) =>
                  val op = a.adminClient.updateDatabaseDdl(a.instance,a.databaes,List(sql),null)
                  op.waitFor().getResult
                  Database.reloadWith(dbClient)
                case None =>
                  _logger.warn("Could not execute administrator query")
              }
              null
          }
        } catch {
          case e : Exception =>
            if (parser.minisql().resultSet != null)
              parser.minisql().resultSet.close()
            throw e
        }
    }.lastOption.orNull

    def executeQuery(sql:String): ResultSet = executeQuery(sql,null)

    /**
      * Internal use
      */
    def isKey(tableName:String,columnName:String):Boolean =
      Database(dbClient).table(tableName).get.isKey(columnName)

    /**
      * Internal use
      */
    def insert(tableName:String,values:java.util.List[Value]):Unit = {
      val m = Mutation.newInsertBuilder(tableName)
      val columns = Database(dbClient).table(tableName) match {
        case Some(tbl) =>
          tbl.columns
        case None =>
          throw new RuntimeException(s"Table not found $tableName")
      }

      columns.zip(values).foreach {
        case (k, NullValue) =>
        case (k, ArrayValue(v)) =>
          m.set(k.name).toStringArray(v)
        case (k, v) =>
          m.set(k.name).to(v.text)
      }
      _transactionContext match {
        case Some(_) =>
          _mutations ++= List(m.build())
        case _ =>
          dbClient.write(List(m.build()))
      }
    }

    def insert(tableName:String,columns:java.util.List[String],values:java.util.List[Value]):Unit = {
      val m = Mutation.newInsertBuilder(tableName)
      columns.zip(values).foreach {
        case (k, NullValue) =>
        case (k, ArrayValue(v)) =>
          m.set(k).toStringArray(v)
        case (k, v) =>
          m.set(k).to(v.text)
      }
      _transactionContext match {
        case Some(_) =>
          _mutations ++= List(m.build())
        case _ =>
          dbClient.write(List(m.build()))
      }
    }

    def bulkInsert(tableName:String,columns:java.util.List[String],values:java.util.List[java.util.List[Value]]):Unit = {
      val mm = values.map {
        v =>
          val m = Mutation.newInsertBuilder(tableName)
          columns.zip(v).foreach(kv=>if (kv._2 != NullValue) m.set(kv._1).to(kv._2.text))
          m.build()
      }

      _transactionContext match {
        case Some(_) =>
          _mutations ++= mm
        case _ =>
          dbClient.write(mm)
      }
    }

    def bulkInsert(tableName:String,values:java.util.List[java.util.List[Value]]):Unit = {
      val m = Mutation.newInsertBuilder(tableName)
      val columns = Database(dbClient).table(tableName) match {
        case Some(tbl) =>
          tbl.columns
        case None =>
          throw new RuntimeException(s"Table not found $tableName")
      }
      val mm = values.map {
        v =>
          val m = Mutation.newInsertBuilder(tableName)
          columns.zip(v).foreach(kv=>if (kv._2 != NullValue) m.set(kv._1.name).to(kv._2.text))
          m.build()
      }

      _transactionContext match {
        case Some(_) =>
          _mutations ++= mm
        case _ =>
          dbClient.write(mm)
      }
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

    private def _getTargetKeys(transaction: TransactionContext,tableName:String,where:String):List[String] = {
      val tbl = Database(dbClient).table(tableName).get
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

        case PrimaryKeyListWhere(k,v) =>
          val mm = v.map {
            vv=>
              val m = Mutation.newUpdateBuilder(tableName)
              m.set(k).to(vv.text)
              keysAndValues.foreach(kv=>m.set(kv.key).to(kv.value))
              m.build()
          }
          _transactionContext match {
            case Some(_) =>
              _mutations ++= mm
            case _ =>
              dbClient.write(mm)
          }

        case NormalWhere(w) =>
          _logger.info(s"Slow update query on $tableName. Reason => $w")
          _transactionContext match {
            case Some(tr) =>
              val keys = _getTargetKeys(tr,tableName,w)
              if (keys.nonEmpty) {
                val key = Database(dbClient).table(tableName).get.key
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
                      val key = Database(dbClient).table(tableName).get.key
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
          _logger.info(s"Slow delete query on $tableName. Reason => $w")
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

    def showTables():ResultSet = {
      dbClient.singleUse().executeQuery(Statement.of("SELECT TABLE_NAME,PARENT_TABLE_NAME,ON_DELETE_ACTION FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=\"\""))
    }

    def showColumns(table:String):ResultSet =
      dbClient.singleUse().executeQuery(Statement.of(s"SELECT COLUMN_NAME,SPANNER_TYPE,IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='$table' ORDER BY ORDINAL_POSITION"))

    /**
      * Internal use
      * To resolve Antlr4's 'throws' keyword issue
      * Currently, when 'throws' is used in the grammar, it will cause strange result on compile time.
      * TODO: Modify to use 'throws' when it is resolved.
      */
    def useNative():Unit =
      throw new NativeSqlException

    def useAdminNative():Unit =
      throw new NativeAdminSqlException

    def unknownFunction(name:String):Unit =
      throw new RuntimeException(s"Unknown function $name")


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

    /**
      * For java
      * @param t
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

    def sql(q:String) = executeQuery(q,null)

    def rollback:Unit = _transactionContext.foreach(_ => _mutations = List.empty[Mutation])
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



  def tryq(q: => ResultSet):Either[SpannerException,ResultSet] = try {
    Right(q)
  } catch {
    case e : SpannerException =>
      Left(e)
  }

//  def executeQuery(sql:String)(implicit dbClient:Nat):ResultSet =
//    dbClient.executeQuery(sql)
}

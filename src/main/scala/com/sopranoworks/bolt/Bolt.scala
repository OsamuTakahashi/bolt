package com.sopranoworks.bolt

import java.io.ByteArrayInputStream

import com.google.cloud.Date
import com.google.cloud.spanner.TransactionRunner.TransactionCallable
import com.google.cloud.spanner._
import org.antlr.v4.runtime._
import org.antlr.v4.runtime.misc.ParseCancellationException
import org.slf4j.LoggerFactory
import shapeless.HNil

import scala.annotation.tailrec
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
    def executeQuery(srcSql:String,admin:Admin,instanceId:String): ResultSet = /*srcSql.split(";").map(_.trim).filter(!_.isEmpty).map */ {
//      sql =>
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
    } //.lastOption.orNull

    def executeQuery(sql:String): ResultSet = executeQuery(sql,null,null)

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
      val evs = values.map(_.eval.asValue)

      columns.zip(evs).foreach {
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
          Option(dbClient).foreach(_.write(List(m.build())))
      }
    }

    def insert(tableName:String,columns:java.util.List[String],values:java.util.List[Value]):Unit = {
      val m = Mutation.newInsertBuilder(tableName)
      val evs = values.map(_.eval.asValue)

      columns.zip(evs).foreach {
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
          Option(dbClient).foreach(_.write(List(m.build())))
      }
    }

    def bulkInsert(tableName:String,columns:java.util.List[String],values:java.util.List[java.util.List[Value]]):Unit = {
      val mm = values.map {
        v =>
          val m = Mutation.newInsertBuilder(tableName)
//          columns.zip(v).foreach(kv=>if (kv._2 != NullValue) m.set(kv._1).to(kv._2.text))
          val evs = v.map(_.eval.asValue)
          columns.zip(evs).foreach {
            case (k, NullValue) =>
            case (k, ArrayValue(v)) =>
              m.set(k).toStringArray(v)
            case (k, v) =>
              m.set(k).to(v.text)
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
      val columns = Database(dbClient).table(tableName) match {
        case Some(tbl) =>
          tbl.columns
        case None =>
          throw new RuntimeException(s"Table not found $tableName")
      }
      val mm = values.map {
        v =>
          val m = Mutation.newInsertBuilder(tableName)
//          columns.zip(v).foreach(kv=>if (kv._2 != NullValue) m.set(kv._1.name).to(kv._2.text))
          val evs = v.map(_.eval.asValue)
          columns.zip(evs).foreach {
            case (k, NullValue) =>
            case (k, ArrayValue(v)) =>
              m.set(k.name).toStringArray(v)
            case (k, v) =>
              m.set(k.name).to(v.text)
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
//          keysAndValues.foreach(kv=>m.set(kv.key).to(kv.value))
          val kve = keysAndValues.map {
            case KeyValue(k,v) =>
              KeyValue(k,v.eval.asValue)
          }
          kve.map(kv=>(kv.key,kv.value)).foreach {
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
              Option(dbClient).foreach(_.write(List(m.build())))
          }

        case PrimaryKeyListWhere(k,v) =>
          val mm = v.map {
            vv=>
              val m = Mutation.newUpdateBuilder(tableName)
              m.set(k).to(vv.text)
//              keysAndValues.foreach(kv=>m.set(kv.key).to(kv.value))
              keysAndValues.map(kv=>(kv.key,kv.value)).foreach {
                case (k, NullValue) =>
                case (k, ArrayValue(v)) =>
                  m.set(k).toStringArray(v)
                case (k, v) =>
                  m.set(k).to(v.text)
              }
              m.build()
          }
          _transactionContext match {
            case Some(_) =>
              _mutations ++= mm
            case _ =>
              Option(dbClient).foreach(_.write(mm))
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
//                    keysAndValues.foreach(kv=>m.set(kv.key).to(kv.value))
                    keysAndValues.map(kv=>(kv.key,kv.value)).foreach {
                      case (k, NullValue) =>
                      case (k, ArrayValue(v)) =>
                        m.set(k).toStringArray(v)
                      case (k, v) =>
                        m.set(k).to(v.text)
                    }
                    m.build()
                }
                _mutations ++= ml
              }

            case _ =>
              Option(dbClient).foreach(_.readWriteTransaction()
                .run(new TransactionCallable[Unit] {
                  override def run(transaction: TransactionContext):Unit = {
                    val keys = _getTargetKeys(transaction,tableName,w)
                    if (keys.nonEmpty) {
                      val key = Database(dbClient).table(tableName).get.key
                      val ml = keys.map {
                        k =>
                          val m = Mutation.newUpdateBuilder(tableName)
                          m.set(key).to(k)
//                          keysAndValues.foreach(kv=>m.set(kv.key).to(kv.value))
                          keysAndValues.map(kv=>(kv.key,kv.value)).foreach {
                            case (k, NullValue) =>
                            case (k, ArrayValue(v)) =>
                              m.set(k).toStringArray(v)
                            case (k, v) =>
                              m.set(k).to(v.text)
                          }
                          m.build()
                      }
                      transaction.buffer(ml)
                    }
                  }
                }))
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
              Option(dbClient).foreach(_.write(List(m)))
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
              Option(dbClient).foreach(_.readWriteTransaction()
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
              b.add("DATABASE_NAME",Value.string(db.getId.getDatabase)).build()
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

    def showCreateTable(tableName:String):ResultSet = {
      val tbl = (List(s"CREATE TABLE $tableName (") ++
        _addComma(showColumns(tableName).map {
          col =>
            s"  ${col.getString("COLUMN_NAME")} ${col.getString("SPANNER_TYPE")} ${if (col.getString("IS_NULLABLE").toUpperCase.startsWith("T")) "" else "NOT NULL"}"
        }.toList) ++
        List(s") ${_primaryKeyForTable(tableName).map(k => s"PRIMARY KEY ($k)").getOrElse("") }", s"${ _deleteActionForTable(tableName).getOrElse(""); };") )
          .map(s=>Struct.newBuilder().add(tableName,Value.string(s)).build())
      ResultSets.forRows(Type.struct(List(Type.StructField.of(tableName,Type.string()))),tbl)
    }

    /**
      * Internal use
      * To resolve Antlr4's 'throws' keyword issue
      * Currently, when 'throws' is used in the grammar, it will cause strange result on compile time.
      * TODO: Modify to use 'throws' when it is resolved.
      */
//    def useNative():Unit =
//      throw new NativeSqlException
//
//    def useAdminNative():Unit =
//      throw new NativeAdminSqlException

    def unknownFunction(name:String):Unit =
      throw new RuntimeException(s"Unknown function $name")

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

//    def sql(q:String) = executeQuery(q,null,null)

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



//  def tryq(q: => ResultSet):Either[SpannerException,ResultSet] = try {
//    Right(q)
//  } catch {
//    case e : SpannerException =>
//      Left(e)
//  }
}

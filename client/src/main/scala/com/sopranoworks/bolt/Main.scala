package com.sopranoworks.bolt

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.spanner._
//import com.typesafe.config.{Config, ConfigFactory}

import scala.io.StdIn
import scala.collection.JavaConverters._

/**
  * Created by takahashi on 2017/05/17.
  */
object Main extends App {
  import Bolt._
//  import codebook.runtime.util.SafeConfig._
  
//  val config = ConfigFactory.load()

  def spannerOptions(config:Options) = {
    val options = SpannerOptions.newBuilder()

    config.password.foreach {
      json =>
        println(s"use client secret:$json")
        val is = new FileInputStream(new File(json))
        options.setCredentials(GoogleCredentials.fromStream(is))
    }
    config.projectId.foreach(options.setProjectId)
    options.build()
  }

  def printRow(columnLength:Array[Int],columns:List[String]):Unit = {
    var i = 0
    columns.foreach {
      col =>
        val l = col.length
        print(col)
        print(" " * (columnLength(i) - l + 1))
        i += 1
    }
    print("\n")
  }


  def showResult(resultSet : ResultSet):Unit = {
    var columnLength = Array.empty[Int]
    var header = List.empty[String]
    var first = true
    try {
      val rows = resultSet.map {
        r =>
          if (first) {
            val f = r.getCurrentRowAsStruct.getType.getStructFields.asScala
            header = f.map(_.getName).toList
            columnLength = header.map(_.length).toArray
            first = false
          }
          (0 until r.getColumnCount).map {
            i =>
              val c = if (r.isNull(i)) "NULL"
              else {
                r.getColumnType(i) match {
                  case v if v == Type.bool() =>
                    r.getBoolean(i).toString
                  case v if v == Type.int64() =>
                    r.getLong(i).toString
                  case v if v == Type.float64() =>
                    r.getDouble(i).toString
                  case v if v == Type.string() =>
                    r.getString(i)
                  case v if v == Type.timestamp() =>
                    r.getTimestamp(i).toString
                  case v if v == Type.date() =>
                    r.getDate(i).toString
                  case v if v == Type.array(Type.bool()) =>
                    s"[${r.getBooleanArray(i).mkString(",")}]"
                  case v if v == Type.array(Type.int64()) =>
                    s"[${r.getLongArray(i).mkString(",")}]"
                  case v if v == Type.array(Type.float64()) =>
                    s"[${r.getDoubleArray(i).mkString(",")}]"
                  case v if v == Type.array(Type.string()) =>
                    s"[${r.getStringList(i).asScala.mkString(",")}]"
                  case v if v == Type.array(Type.timestamp()) =>
                    s"[${r.getTimestampList(i).asScala.mkString(",")}]"
                  case v if v == Type.array(Type.date()) =>
                    s"[${r.getDateList(i).asScala.mkString(",")}]"
                  case t =>
                    s"Unknown($t)"
                }
              }
              val l = c.length
              if (l > columnLength(i)) columnLength(i) = l
              c
          }.toList
      }.toList

      printRow(columnLength,header)
      printRow(columnLength,header.map("-" * _.length))
      rows.foreach(r=>printRow(columnLength,r))
    } finally {
      resultSet.close()
    }
  }

  private def _removeComment(line:String):String = {
    val i = line.indexOf("--")
    if (i >= 0) {
      line.substring(0,i)
    } else {
      line
    }
  }

  case class Options(projectId:Option[String] = None,instanceName:Option[String] = None,database:Option[String] = None,createDatabase:Boolean = false,password:Option[String] = None,sqls:Seq[File] = Seq.empty[File])

  val optParser = new scopt.OptionParser[Options]("spanner-cli") {
    opt[String]('p',"projectId").action((x,c) => c.copy(projectId = Some(x)))
    opt[String]('i',"instance").action((x,c) => c.copy(instanceName = Some(x)))
    opt[String]('s',"secret").action((x,c) => c.copy(password = Some(x)))
    opt[Unit]('c',"create").action((_,c) => c.copy(createDatabase = true))
    arg[String]("database").optional().action((x,c) => c.copy(database = Some(x)))
  }

  optParser.parse(args,Options()) match {
    case Some(cfg) =>
      val options = spannerOptions(cfg)
      val spanner = options.getService
      val instance = cfg.instanceName.getOrElse {
        println("Instance name must be specified")
        optParser.showUsage()
        System.exit(1)
        ""
      }
      var dbName = cfg.database
      var displayName = dbName.getOrElse("")

      var admin = Admin(spanner.getDatabaseAdminClient,instance,dbName.orNull)
      if (cfg.createDatabase && admin != null) {
        admin.adminClient.createDatabase(instance,dbName.get,List.empty[String].asJava).waitFor()
      }

      var dbClient = dbName.map(dbn=>spanner.getDatabaseClient(DatabaseId.of(options.getProjectId, instance, dbn))).orNull
      Option(dbClient).foreach(Database.startWith)

      val reader = new jline.console.ConsoleReader()
      val stream = new BufferedReader(new InputStreamReader(System.in))

      var prevString = ""

      val p = System.console() != null

      val it = if (!p) Iterator.continually(stream.readLine())
        else Iterator.continually(reader.readLine(s"${if (prevString.nonEmpty) " " * displayName.length + "| " else displayName + "> "}"))

        it.takeWhile(l => l != null && l != "exit").foreach {
          line =>
            prevString += " " + _removeComment(line)
            prevString = prevString
            var i = 0

            while (i >= 0) {
              i = prevString.indexOf(';')
              if (i >= 0) {
                val sql = prevString.substring(0, i)
                prevString = prevString.drop(i + 1)

                if (sql.nonEmpty) {
                  try {
                    Option(new Nat(dbClient).executeQuery(sql, admin, instance)).foreach {
                      res =>
                        showResult(res)
                        res.close()
                    }
                  } catch {
                    case e: com.google.cloud.spanner.SpannerException =>
                      println(e.getMessage)

                    case DatabaseChangedException(newDbName) =>
                      try {
                        val cln = spanner.getDatabaseClient(DatabaseId.of(options.getProjectId, instance, newDbName))
                        Option(cln).foreach(Database.startWith)

                        dbClient = cln
                        admin = Admin(spanner.getDatabaseAdminClient, instance, newDbName)
                        dbName = Some(newDbName)
                        displayName = newDbName
                      } catch {
                        case e: SpannerException =>
                          println(e.getMessage)
                      }

                    case e: RuntimeException =>
                      println(e.getMessage)
                  }
                }
              }
            }
        }
      spanner.close()
    case None =>
  }
}

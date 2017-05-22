package com.sopranoworks.bolt

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.spanner._
import com.typesafe.config.{Config, ConfigFactory}

import scala.io.StdIn
import scala.collection.JavaConverters._

/**
  * Created by takahashi on 2017/05/17.
  */
object Main extends App {
  import Bolt._
  import codebook.runtime.util.SafeConfig._
  
  val config = ConfigFactory.load()

  def spannerOptions(config:Config) = {
    val options = SpannerOptions.newBuilder()

    config.getStringOpt("spanner.client_secret").foreach {
      json =>
        println(s"use client secret:$json")
        val is = new FileInputStream(new File(json))
        options.setCredentials(GoogleCredentials.fromStream(is))
    }
    options.build()
  }

  def showResult(resultSet : ResultSet):Unit = {
    var first = true
    try {
      resultSet.foreach {
        r =>
          if (first) {
            val f = r.getCurrentRowAsStruct.getType.getStructFields
            println(f.asScala.map(_.getName).mkString("\t"))
            first = false
          }
          println((0 until r.getColumnCount).map {
            i =>
              if (r.isNull(i)) "NULL"
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
                  case t =>
                    s"Unknown($t)"

                }
              }
          }.mkString("\t"))
      }
    } finally {
      resultSet.close()
    }
  }

  case class Options(instanceName:Option[String] = None,database:String = "",createDatabase:Boolean = false,password:Option[String] = None,sqls:Seq[File] = Seq.empty[File])

  val optParser = new scopt.OptionParser[Options]("spanner-cli") {
    opt[String]('i',"instance").action((x,c) => c.copy(instanceName = Some(x)))
    opt[String]('s',"secret").action((x,c) => c.copy(password = Some(x)))
    opt[Unit]('c',"create").action((_,c) => c.copy(createDatabase = true))
    arg[String]("database").action((x,c) => c.copy(database = x))
//    arg[File]("<sql files> ...").unbounded().optional().action((x,c) => c.copy(sqls = c.sqls :+ x))
  }

  optParser.parse(args,Options()) match {
    case Some(cfg) =>
      val options = spannerOptions(config)
      val spanner = options.getService
      val instance = cfg.instanceName.get//config.getString("spanner.instance")
      val dbName = cfg.database//config.getString("spanner.database")

      val admin = Admin(spanner.getDatabaseAdminClient,instance,dbName)
      if (cfg.createDatabase) {
        admin.adminClient.createDatabase(instance,dbName,List.empty[String].asJava).waitFor()
      }

      val dbClient = spanner.getDatabaseClient(DatabaseId.of(options.getProjectId, instance, dbName))
      Database.startWith(dbClient)

      val reader = new jline.console.ConsoleReader()
      val stream = new BufferedReader(new InputStreamReader(System.in))

      var prevString = ""

      val p = System.console() != null

      val it = if (!p) Iterator.continually(stream.readLine())
        else Iterator.continually(reader.readLine(s"${if (prevString.nonEmpty) " " * dbName.length + "| " else dbName + "> "}"))

//      Iterator
//        .continually(reader.readLine(s"${if (p) {if (prevString.nonEmpty) " " * dbName.length + "| " else dbName + "> "}  else ""}"))
//        .withFilter(l => l != null && !l.isEmpty)
        it.takeWhile(l => l != null && l != "exit")
        .foreach {
          line =>
            prevString += " " + line
            prevString = prevString
            var i = 0

            while (i >= 0) {
              i = prevString.indexOf(';')
              if (i >= 0) {
                val sql = prevString.substring(0, i)
                prevString = prevString.drop(i + 1)

                if (sql.nonEmpty) {
                  try {
                    Option(dbClient.executeQuery(sql, admin)).foreach {
                      res =>
                        showResult(res)
                        res.close()
                    }
                  } catch {
                    case e: com.google.cloud.spanner.SpannerException =>
                      println(e.getMessage)
                    case e: RuntimeException =>
                      println(e.getMessage)
                  }
                }
              }
            }
        }
      spanner.closeAsync()
    case None =>
  }
}

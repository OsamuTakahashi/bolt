/**
  * Bolt
  * Spanner-dump
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt

import java.io.{File, FileInputStream}

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.spanner._

import scala.collection.JavaConverters._

object Main extends App {
  import Bolt._

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

  def makeResult(resultSet : ResultSet):List[List[String]] = {
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
                    s"'${r.getString(i)}'"
                  case v if v == Type.timestamp() =>
                    s"'${r.getTimestamp(i).toString}'"
                  case v if v == Type.date() =>
                    s"'${r.getDate(i).toString}'"
                  case v if v == Type.array(Type.bool()) =>
                    s"[${r.getBooleanArray(i).mkString(",")}]"
                  case v if v == Type.array(Type.int64()) =>
                    s"[${r.getLongArray(i).mkString(",")}]"
                  case v if v == Type.array(Type.float64()) =>
                    s"[${r.getDoubleArray(i).mkString(",")}]"
                  case v if v == Type.array(Type.string()) =>
                    s"[${r.getStringList(i).asScala.map(s=>s"'$s'").mkString(",")}]"
                  case v if v == Type.array(Type.timestamp()) =>
                    s"[${r.getTimestampList(i).asScala.map(s=>s"'$s'").mkString(",")}]"
                  case v if v == Type.array(Type.date()) =>
                    s"[${r.getDateList(i).asScala.map(s=>s"'$s'").mkString(",")}]"
                  case t =>
                    s"Unknown($t)"
                }
              }
              val l = c.length
              if (l > columnLength(i)) columnLength(i) = l
              c
          }.toList
      }.toList
      rows

    } finally {
      resultSet.close()
    }
  }
  case class Options(projectId:Option[String] = None,instanceName:Option[String] = None,database:Option[String] = None,table:Option[String] = None,password:Option[String] = None,sqls:Seq[File] = Seq.empty[File])

  val optParser = new scopt.OptionParser[Options]("spanner-cli") {
    opt[String]('p',"projectId").action((x,c) => c.copy(projectId = Some(x)))
    opt[String]('i',"instance").action((x,c) => c.copy(instanceName = Some(x)))
    opt[String]('s',"secret").action((x,c) => c.copy(password = Some(x)))
    opt[String]('d',"database").action((x,c) => c.copy(database = Some(x)))
    arg[String]("table").optional().action((x,c) => c.copy(table = Some(x)))
  }

  def dumpTable(dbClient:DatabaseClient,nat:Nat,tbl:String):Unit = {
    makeResult(nat.showCreateTable(tbl)).foreach(row=>println(row.map(_.init.tail).mkString("")))
    var loop = true
    var offset = 0
    while(loop) {
      val r = makeResult(dbClient.singleUse().executeQuery(Statement.of(s"SELECT * from $tbl LIMIT 100 OFFSET $offset ")))
      if (r.nonEmpty) {
        println(s"INSERT INTO $tbl VALUES")
        println(r.map(row => s"  (${row.mkString(",")})").mkString(",\n") + ";")
      }
      loop = r.length == 100
      offset += 100
    }
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
      var dbName = cfg.database.getOrElse {
        println("Database name must be specified")
        optParser.showUsage()
        System.exit(1)
        ""
      }
      var admin = Admin(spanner.getDatabaseAdminClient, instance, dbName)
      var dbClient = spanner.getDatabaseClient(DatabaseId.of(options.getProjectId, instance, dbName))
      val nat = Nat(dbClient)

      cfg.table match {
        case Some(tbl) =>
          dumpTable(dbClient,nat,tbl)
        case None =>
          val tbls = makeResult(dbClient.singleUse().executeQuery(Statement.of("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=\"\"")))
          tbls.foreach(tbl=>dumpTable(dbClient,nat,tbl.head.init.tail))
      }
      spanner.close()
  }
}

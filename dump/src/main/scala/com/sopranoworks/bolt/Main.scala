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
import com.sopranoworks.bolt.values.BytesValue
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig
import org.apache.commons.text.StringEscapeUtils

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

  def escapedString(str:String):String =
    s"""\"${StringEscapeUtils.escapeJava(str)}\""""

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
                    escapedString(r.getString(i))
                  case v if v == Type.bytes() =>
                    s"'${BytesValue(r.getBytes(i).toByteArray).text}'"
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
                    s"[${r.getStringList(i).asScala.map(s=>escapedString(s)).mkString(",")}]"
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
  case class Options(projectId:Option[String] = None,
                     instanceName:Option[String] = None,
                     database:Option[String] = None,
                     table:Option[String] = None,
                     password:Option[String] = None,
                     sqls:Seq[File] = Seq.empty[File],
                     noInfo:Boolean = false,
                     noData:Boolean = false,
                     userDataFlow:Boolean = false,
                     dataStoreBase:Option[String] = None,
                     runner:String = "DataflowRunner",
                     workerMachineType:String = "n1-standard-1",
                     maxNumWorkers:Int = 1,
                     region:Option[String] = None)

  val optParser = new scopt.OptionParser[Options]("spanner-dump") {
    opt[String]('p',"projectId").action((x,c) => c.copy(projectId = Some(x)))
    opt[String]('i',"instance").action((x,c) => c.copy(instanceName = Some(x)))
    opt[String]('s',"secret").action((x,c) => c.copy(password = Some(x)))
//    opt[String]('d',"database").action((x,c) => c.copy(database = Some(x)))
    opt[Unit]('t',"no-create-info").action((_, c) => c.copy(noInfo = true))
    opt[Unit]('d',"no-data").action((_, c) => c.copy(noData = true))
    opt[Unit]("dataflow").optional().action((_,c) => c.copy(userDataFlow = true))
    opt[String]("datastore-base").optional().action((x,c) => c.copy(dataStoreBase = Some(x)))
    opt[String]("runner").optional().action((x,c) => c.copy(runner = x))
    opt[String]("workerMachineType").optional().action((x,c) => c.copy(workerMachineType = x))
    opt[Int]("maxNumWorkers").optional().action((x,c) => c.copy(maxNumWorkers = x))
    opt[String]("region").optional().action((x,c) => c.copy(region = Some(x)))
    arg[String]("db_name").action((x,c) => c.copy(database = Some(x)))
    arg[String]("table").optional().action((x,c) => c.copy(table = Some(x)))
  }

  def dumpTable(dbClient:DatabaseClient, nut:Nut, tbl:String, noInfo:Boolean, noData:Boolean,useDataFlow:Boolean,dataFlowArgs:Array[String],spannerConfig: Option[SpannerConfig]):Unit = {
    if (!noInfo) {
      makeResult(nut.showCreateTable(tbl)).foreach(row => println(row.map(_.init.tail).mkString("")))
    }
    if (!noData) {
      if (useDataFlow) {
        DataFlowDumper.dump(dataFlowArgs,spannerConfig.get,nut.database.table(tbl).get)
      } else {
        var loop = !noData
        var offset = 0
        while (loop) {
          val r = makeResult(dbClient.singleUse().executeQuery(Statement.of(s"SELECT * from $tbl LIMIT 100 OFFSET $offset ")))
          if (r.nonEmpty) {
            println(s"INSERT INTO $tbl VALUES")
            println(r.map(row => s"  (${row.mkString(",")})").mkString(",\n") + ";")
          }
          loop = r.length == 100
          offset += 100
        }
      }
    }
  }

  def tableDependencyList(dbClient:DatabaseClient):List[String] = {
    var res = List.empty[String]
    var tableSet = Set.empty[String]
    val tbls = dbClient.singleUse().executeQuery(Statement.of("SELECT TABLE_NAME,PARENT_TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=\"\""))
      .autoclose(_.map(res=>(res.getString(0),res.getStringOpt("PARENT_TABLE_NAME"))).toList)
      .sortWith((a,b) => a._2.getOrElse("").compareTo(b._2.getOrElse("")) < 0)

    var last = 0

    while (tableSet.size != tbls.length) {
      tbls.foreach {
        case (n, None) =>
          if (!tableSet.contains(n)) {
            tableSet += n
            res ::= n
          }
        case (n, Some(p)) =>
          if (!tableSet.contains(n) && tableSet.contains(p)) {
            tableSet += n
            res ::= n
          }
      }

      if (last == tableSet.size) throw new RuntimeException("Cyclic table dependency")
      last = tableSet.size
    }
    res.reverse
  }

  optParser.parse(args,Options()) match {
    case Some(cfg) =>
      val options = spannerOptions(cfg)
      val spanner = options.getService
      val instance = cfg.instanceName.getOrElse {
        println("Instance name must be specified")
        println(optParser.usage)
        System.exit(1)
        ""
      }
      var dbName = cfg.database.getOrElse {
        println("Database name must be specified")
        println(optParser.usage)
        System.exit(1)
        ""
      }
      var spannerConfig:Option[SpannerConfig] = None
      val dataFlowArgs = if (cfg.userDataFlow) {
        if (cfg.projectId.isEmpty || cfg.dataStoreBase.isEmpty || cfg.region.isEmpty) {
          println(optParser.usage)
          System.exit(1)
          Array.empty[String]
        } else {
          spannerConfig = Some(SpannerConfig.create()
            .withProjectId(cfg.projectId.get)
            .withInstanceId(cfg.instanceName.get)
            .withDatabaseId(cfg.database.get))

          Array(
            s"--project=${cfg.projectId.get}",
            s"--region=${cfg.region.get}",
            s"--runner=${cfg.runner}",
            s"--workerMachineType=${cfg.workerMachineType}",
            s"--maxNumWorkers=${cfg.maxNumWorkers}",
            s"--output=${cfg.dataStoreBase.get}"
          )
        }
      } else {
        Array.empty[String]
      }
      var admin = Admin(spanner.getDatabaseAdminClient, instance, dbName)
      var dbClient = spanner.getDatabaseClient(DatabaseId.of(options.getProjectId, instance, dbName))
      val nut = Nut(dbClient)

      cfg.table match {
        case Some(tbl) =>
          dumpTable(dbClient,nut,tbl,cfg.noInfo,cfg.noData,cfg.userDataFlow,dataFlowArgs,spannerConfig)
        case None =>
          val tbls = tableDependencyList(dbClient)
          tbls.foreach(tbl=>dumpTable(dbClient,nut,tbl,cfg.noInfo,cfg.noData,cfg.userDataFlow,dataFlowArgs,spannerConfig))
      }
      spanner.close()
    case None =>
  }
}

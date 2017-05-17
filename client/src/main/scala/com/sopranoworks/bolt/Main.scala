package com.sopranoworks.bolt

import java.io.{File, FileInputStream}

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

  val options = spannerOptions(config)
  val spanner = options.getService
  val dbName = config.getString("spanner.database")

  val dbClient = spanner.getDatabaseClient(DatabaseId.of(options.getProjectId, config.getString("spanner.instance"), dbName))
  Database.startWith(dbClient)

  Iterator
    .continually(StdIn.readLine(s"$dbName> "))
    .withFilter(l => l != null && !l.isEmpty)
    .takeWhile(_ != "exit")
    .foreach {
      sql =>
        try {
          Option(dbClient.executeQuery(sql)).foreach {
            res =>
              showResult(res)
              res.close()
          }
        } catch {
          case e : com.google.cloud.spanner.SpannerException =>
            println(e.getMessage)
          case e : RuntimeException =>
            println(e.getMessage)
        }
    }
  spanner.closeAsync()
}

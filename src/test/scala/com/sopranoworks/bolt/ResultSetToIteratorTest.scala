package com.sopranoworks.bolt

import java.io.{File, FileInputStream}

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.spanner.{DatabaseClient, DatabaseId, SpannerOptions}
import com.typesafe.config.{Config, ConfigFactory}
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterEach

/**
  * Created by takahashi on 2017/04/10.
  */
class ResultSetToIteratorTest extends Specification with BeforeAfterEach {
  sequential

  import Bolt._
  //import codebook.runtime.util.SafeConfig._

  private var _options:Option[SpannerOptions] = None
  private var _dbClient:Option[DatabaseClient] = None

  def spannerOptions(config:Config) = {
    val options = SpannerOptions.newBuilder()
//    options.setProjectId()

    /*config.getStringOpt("spanner.client_secret").foreach {
      json =>
        println(s"use client secret:$json")
        val is = new FileInputStream(new File(json))
        options.setCredentials(GoogleCredentials.fromStream(is))
    } */
    options.build()
  }

  override protected def before: Any = {
    val config = ConfigFactory.load()
    val options = spannerOptions(config)

    _options = Some(options)
    val spanner = options.getService
    val dbClient = spanner.getDatabaseClient(DatabaseId.of(options.getProjectId, config.getString("spanner.instance"), config.getString("spanner.database")))
    Database.startWith(dbClient)
    _dbClient = Some(dbClient)
    _dbClient.foreach(_.executeQuery("DELETE FROM test_tbl01"))
    _dbClient.foreach(_.executeQuery("DELETE FROM test_items"))
  }

  override protected def after: Any = {
    _options.foreach(_.getService.close())
  }

//  "Iteration test" should {
//    "map" in {
//      _dbClient.get.executeQuery("INSERT INTO test_tbl01 (id,name) VALUES(103,'test insert');")
//      val resSet = _dbClient.get.executeQuery("SELECT * FROM test_tbl01 WHERE id=103")
//      resSet.map(r=>r.getString("name")).toList must_== List("test insert")
////      println("begin")
////      Thread.sleep(300000)
////      true must_== true
//    }
//  }
////  "error handling test" should {
////    "null get" in {
////      _dbClient.get.sql("INSERT INTO test_tbl01 (id,name) VALUES(103,'test insert');")
////      val resSet = _dbClient.get.sql("SELECT * FROM test_tbl01 WHERE id=103")
////      resSet.map(r=>r.getString("nocolumn")).toList must_== List("test insert")
////    }
////  }
}
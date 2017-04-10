package com.sopranoworks.bolt

import com.google.cloud.spanner.{DatabaseClient, DatabaseId, SpannerOptions}
import com.typesafe.config.ConfigFactory
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterEach

/**
  * Created by takahashi on 2017/04/10.
  */
class ResultSetToIteratorTest extends Specification with BeforeAfterEach {
  sequential

  import Bolt._

  private var _options:Option[SpannerOptions] = None
  private var _dbClient:Option[DatabaseClient] = None

  override protected def before: Any = {
    val config = ConfigFactory.load()
    val options = SpannerOptions.newBuilder().build()
    _options = Some(options)
    val spanner = options.getService
    val dbClient = spanner.getDatabaseClient(DatabaseId.of(options.getProjectId, config.getString("spanner.instance"), config.getString("spanner.database")))
    Database.beginWith(dbClient)
    _dbClient = Some(dbClient)
    _dbClient.foreach(_.executeQuery("DELETE test_tbl01"))
    _dbClient.foreach(_.executeQuery("DELETE test_items"))
  }

  override protected def after: Any = {
    _options.foreach(_.getService.closeAsync().get())
  }

  "Iteration test" should {
    "map" in {
      _dbClient.get.sql("INSERT INTO test_tbl01 (id,name) VALUES(103,'test insert');")
      val resSet = _dbClient.get.sql("SELECT * FROM test_tbl01 WHERE id=103")
      resSet.map(r=>r.getString("name")).toList must_== List("test insert")
    }
  }
  "error handling test" should {
    "null get" in {
      _dbClient.get.sql("INSERT INTO test_tbl01 (id,name) VALUES(103,'test insert');")
      val resSet = _dbClient.get.sql("SELECT * FROM test_tbl01 WHERE id=103")
      resSet.map(r=>r.getString("nocolumn")).toList must_== List("test insert")
    }
  }
}
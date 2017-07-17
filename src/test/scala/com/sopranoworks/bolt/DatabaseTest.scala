package com.sopranoworks.bolt

import com.google.cloud.spanner.{DatabaseClient, DatabaseId, SpannerOptions}
import com.typesafe.config.ConfigFactory
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterEach

/**
  * Created by takahashi on 2017/03/29.
  */
class DatabaseTest extends Specification with BeforeAfterEach {
  sequential
  
  private var _dbClient:Option[DatabaseClient] = None
  private var _options:Option[SpannerOptions] = None

  override protected def before: Any = {
    val config = ConfigFactory.load()
    val options = SpannerOptions.newBuilder().build()
    _options = Some(options)
    val spanner = options.getService
    _dbClient = Some(spanner.getDatabaseClient(DatabaseId.of(options.getProjectId, config.getString("spanner.instance"), config.getString("spanner.database"))))
  }

  override protected def after: Any = {
    _options.foreach(_.getService.close())
  }

//  "database and tables" should {
//    "normally success" in {
//      val spanner = _options.get.getService
//      val dbClient = spanner.getDatabaseClient(DatabaseId.of(_options.get.getProjectId, "sgspannertest01", "testdb"))
//      val db = Database(dbClient)
//      val tbl = db.table("test_tbl01")
//      tbl.isKey("id") must_== true
//      tbl.isKey("name") must_!= true
//      tbl.column(0) must_== "id"
//      tbl.column(1) must_== "name"
//    }
//  }


  "loadInformationSchema" should {
    "normally success" in {
      val db = Database(_dbClient.get)

      val tbl = db.table("test_tbl01").get
      tbl.isKey("id") must_== true
      tbl.isKey("name") must_!= true
      tbl.column(0).name must_== "id"
      tbl.column(1).name must_== "name"
    }
  }
}

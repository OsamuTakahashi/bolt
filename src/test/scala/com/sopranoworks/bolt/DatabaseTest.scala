package com.sopranoworks.bolt

import com.google.cloud.spanner.{DatabaseId, SpannerOptions}
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterEach

/**
  * Created by takahashi on 2017/03/29.
  */
class DatabaseTest extends Specification with BeforeAfterEach {
  sequential
  
  private var _options:Option[SpannerOptions] = None

  override protected def before: Any = {
    val options = SpannerOptions.newBuilder().build()
    _options = Some(options)
  }

  override protected def after: Any = {
    _options.foreach(_.getService.closeAsync().get())
  }

  "database and tables" should {
    "normally success" in {
      val spanner = _options.get.getService
      val dbClient = spanner.getDatabaseClient(DatabaseId.of(_options.get.getProjectId, "sgspannertest01", "testdb"))
      val db = Database(dbClient)
      val tbl = db.table("test_tbl01")
      tbl.isKey("id") must_== true
      tbl.isKey("name") must_!= true
      tbl.column(0) must_== "id"
      tbl.column(1) must_== "name"
    }
  }
}

package com.sopranoworks.bolt

import com.google.cloud.spanner.{DatabaseClient, DatabaseId, SpannerOptions, Statement}
import com.typesafe.config.ConfigFactory
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterEach

/**
  * Created by takahashi on 2017/03/29.
  */
class NatTest extends Specification with BeforeAfterEach {
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
    _dbClient = Some(dbClient)
    _dbClient.foreach(_.executeQuery("DELETE test_tbl01"))
  }

  override protected def after: Any = {
    _options.foreach(_.getService.closeAsync().get())
  }

  "INSERT" should {
    "normally success" in {
      import Bolt._

      _dbClient.get.executeQuery("INSERT INTO test_tbl01 VALUES(103,'test insert');")
      val resSet = _dbClient.get.singleUse().executeQuery(Statement.of("SELECT * FROM test_tbl01 WHERE id=103"))

      var res = false
      while(resSet.next()) {
        res = resSet.getString("name") == "test insert"
      }
      _dbClient.get.executeQuery("DELETE FROM test_tbl01 WHERE id=103;")

      res must_== true
    }
  }
  "UPDATE" should {
    "normally success" in {
      _dbClient.get.executeQuery("INSERT INTO test_tbl01 VALUES(103,'test insert');")
      val resSet = _dbClient.get.singleUse().executeQuery(Statement.of("SELECT * FROM test_tbl01 WHERE id=103"))

      var res = false
      while(resSet.next()) {
        res = resSet.getString("name") == "test insert"
      }
      res must_== true

      _dbClient.get.executeQuery("UPDATE test_tbl01 SET name='new name' WHERE id=103;")

      val resSet2 = _dbClient.get.singleUse().executeQuery(Statement.of("SELECT * FROM test_tbl01 WHERE id=103"))

      var res2 = false
      while(resSet2.next()) {
        res2 = resSet2.getString("name") == "new name"
      }

      _dbClient.get.executeQuery("DELETE FROM test_tbl01 WHERE id=103;")

      res2 must_== true
    }
    "multiple rows" in {
      _dbClient.get.executeQuery("INSERT INTO test_tbl01 VALUES(101,'user1');")
      _dbClient.get.executeQuery("INSERT INTO test_tbl01 VALUES(102,'user2');")
      _dbClient.get.executeQuery("INSERT INTO test_tbl01 VALUES(103,'user3');")

      _dbClient.get.executeQuery("UPDATE test_tbl01 SET name='new name' WHERE id>100;")

      val resSet2 = _dbClient.get.singleUse().executeQuery(Statement.of("SELECT * FROM test_tbl01"))

      var res2 = false
      while(resSet2.next()) {
        res2 = resSet2.getString("name") == "new name"
      }

      _dbClient.get.executeQuery("DELETE FROM test_tbl01 WHERE id>100;")

      res2 must_== true
    }
    "multiple rows 2" in {
      _dbClient.get.executeQuery("INSERT INTO test_tbl01 VALUES(101,'user1');")
      _dbClient.get.executeQuery("INSERT INTO test_tbl01 VALUES(102,'user2');")
      _dbClient.get.executeQuery("INSERT INTO test_tbl01 VALUES(103,'user3');")

      _dbClient.get.executeQuery("UPDATE test_tbl01 SET name='new name' WHERE id=101 OR id=103;")

      val resSet2 = _dbClient.get.singleUse().executeQuery(Statement.of("SELECT * FROM test_tbl01"))

      var count = 0
      while(resSet2.next()) {
        if (resSet2.getString("name") == "new name")
          count += 1
      }

      _dbClient.get.executeQuery("DELETE FROM test_tbl01 WHERE id>100;")

      count must_== 2
    }
    "not primary key" in {
      _dbClient.get.executeQuery("INSERT INTO test_tbl01 VALUES(101,'user1');")
      _dbClient.get.executeQuery("INSERT INTO test_tbl01 VALUES(102,'user2');")
      _dbClient.get.executeQuery("INSERT INTO test_tbl01 VALUES(103,'user3');")

      _dbClient.get.executeQuery("UPDATE test_tbl01 SET name='new name' WHERE name='user1';")

      val resSet2 = _dbClient.get.singleUse().executeQuery(Statement.of("SELECT * FROM test_tbl01"))

      var count = 0
      while(resSet2.next()) {
        if (resSet2.getString("name") == "new name")
          count += 1
      }

      _dbClient.get.executeQuery("DELETE FROM test_tbl01 WHERE id>100;")

      count must_== 1
    }
    "not primary key multi row" in {
      _dbClient.get.executeQuery("INSERT INTO test_tbl01 VALUES(101,'user1');")
      _dbClient.get.executeQuery("INSERT INTO test_tbl01 VALUES(102,'user2');")
      _dbClient.get.executeQuery("INSERT INTO test_tbl01 VALUES(103,'user3');")

      _dbClient.get.executeQuery("UPDATE test_tbl01 SET name='new name' WHERE name='user1' OR name='user2';")

      val resSet2 = _dbClient.get.singleUse().executeQuery(Statement.of("SELECT * FROM test_tbl01"))

      var count = 0
      while(resSet2.next()) {
        if (resSet2.getString("name") == "new name")
          count += 1
      }

      _dbClient.get.executeQuery("DELETE FROM test_tbl01 WHERE id>100;")

      count must_== 2
    }
  }
  "DELETE" should {
    "multiple rows" in {
      _dbClient.get.executeQuery("INSERT INTO test_tbl01 VALUES(101,'user1');")
      _dbClient.get.executeQuery("INSERT INTO test_tbl01 VALUES(102,'user2');")
      _dbClient.get.executeQuery("INSERT INTO test_tbl01 VALUES(103,'user3');")

      _dbClient.get.executeQuery("DELETE FROM test_tbl01 WHERE id=101 OR id=104;")

      val resSet2 = _dbClient.get.singleUse().executeQuery(Statement.of("SELECT * FROM test_tbl01"))

      var count = 0
      while(resSet2.next()) {
        count += 1
      }
      count must_== 2
    }
  }
}

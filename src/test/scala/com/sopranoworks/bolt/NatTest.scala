package com.sopranoworks.bolt

import com.google.cloud.spanner._
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
    Database.startWith(dbClient)
    _dbClient = Some(dbClient)
    _dbClient.foreach(_.executeQuery("DELETE test_tbl01"))
    _dbClient.foreach(_.executeQuery("DELETE test_items"))
  }

  override protected def after: Any = {
    _options.foreach(_.getService.close())
  }

  case class Item(trId:Long,userId:Long,itemId:Long,count:Int)

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
    "multiple insert" in {
      _dbClient.get.sql("INSERT INTO test_items VALUES(0,0,0,100);")
      _dbClient.get.sql("INSERT INTO test_items VALUES(1,0,1,100);")
      _dbClient.get.sql("INSERT INTO test_items VALUES(2,0,2,100);")
      _dbClient.get.sql("INSERT INTO test_items VALUES(3,0,3,100);")

      val resSet2 = _dbClient.get.singleUse().executeQuery(Statement.of("SELECT * FROM test_items"))

      var count = 0
      while(resSet2.next()) {
        count += 1
      }
      count must_== 4
    }
    "multiple insert 2" in {
      _dbClient.get.sql("INSERT INTO test_items (id,uid,iid,count) VALUES(0,0,0,100);")
      _dbClient.get.sql("INSERT INTO test_items (id,uid,iid,count) VALUES(1,0,1,100);")
      _dbClient.get.sql("INSERT INTO test_items (id,uid,iid,count) VALUES(2,0,2,100);")
      _dbClient.get.sql("INSERT INTO test_items (id,uid,iid,count) VALUES(3,0,3,100);")

      _dbClient.get.sql("INSERT INTO test_items (id,uid,iid,count) VALUES(4,0,4,100)")
      _dbClient.get.sql("INSERT INTO test_items (id,uid,iid,count) VALUES(5,0,5,100)")
      _dbClient.get.sql("INSERT INTO test_items (id,uid,iid,count) VALUES(6,0,6,100)")
      _dbClient.get.sql("INSERT INTO test_items (id,uid,iid,count) VALUES(7,0,7,100)")

      val resSet2 = _dbClient.get.singleUse().executeQuery(Statement.of("SELECT * FROM test_items"))

      var count = 0
      while(resSet2.next()) {
        count += 1
      }
      count must_== 8
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

      val resSet2 = _dbClient.get.executeQuery("SELECT * FROM test_tbl01")

      var count = 0
      while(resSet2.next()) {
        count += 1
      }
      count must_== 2
    }
  }
  "beginTransaction" should {
    "normally success" in {
      _dbClient.get.executeQuery("INSERT INTO test_tbl01 VALUES(101,'user1');")
      _dbClient.get.executeQuery("INSERT INTO test_tbl01 VALUES(102,'user2');")
      _dbClient.get.executeQuery("INSERT INTO test_tbl01 VALUES(103,'user3');")

      var count0 = 0
      _dbClient.get.beginTransaction {
        db =>
          val resSet = db.sql("SELECT * FROM test_tbl01;")
          while(resSet.next()) {
            db.sql(s"UPDATE test_tbl01 set name='new name' WHERE id=${resSet.getLong(0)};")
            count0 += 1
          }
      }
      count0 must_== 3

      val resSet2 = _dbClient.get.executeQuery("SELECT * FROM test_tbl01 WHERE name='new name';")

      println("done")
      var count = 0
//      try {
//        resSet2.map(_ => count += 1)
////        for {
////          r <- resSet2.iterator
////        } yield {
////          println("*")
////          count += 1
////        }
//      } catch {
//        case e:SpannerException =>
//          println(e.getMessage)
//      }
      while(resSet2.next()) {
        count += 1
      }
      count must_== 3
    }
     "multiple commit" in {
      _dbClient.get.beginTransaction {
        db =>
          db.sql("INSERT INTO test_items (id,uid,iid,count) VALUES(0,0,0,100);")
          db.sql("INSERT INTO test_items (id,uid,iid,count) VALUES(1,0,1,100);")
          db.sql("INSERT INTO test_items (id,uid,iid,count) VALUES(2,0,2,100);")
          db.sql("INSERT INTO test_items (id,uid,iid,count) VALUES(3,0,3,100);")
      }

      _dbClient.get.beginTransaction {
        db =>
          val resSet = db.sql(s"SELECT * FROM test_items WHERE uid=0")
          while(resSet.next()) {}

          db.sql("INSERT INTO test_items (id,uid,iid,count) VALUES(4,0,4,100)")
          db.sql("INSERT INTO test_items (id,uid,iid,count) VALUES(5,0,5,100)")
          db.sql("INSERT INTO test_items (id,uid,iid,count) VALUES(6,0,6,100)")
          db.sql("INSERT INTO test_items (id,uid,iid,count) VALUES(7,0,7,100)")
      }
      val resSet2 = _dbClient.get.executeQuery("SELECT * FROM test_items;")
      var count = 0
      while(resSet2.next()) {
        count += 1
      }
      count must_== 8
    }
    "multiple commit 2" in {
      val table = "test_items"
      val uid = 0L
      var n = 0
      while(n < 12) {
        _dbClient.get.beginTransaction {
          db =>
            val resSet = db.sql(s"SELECT * FROM $table WHERE uid=$uid")
            var r = List.empty[Item]
            while (resSet.next()) {
                r ::= Item(resSet.getLong(0), resSet.getLong(1), resSet.getLong(2), resSet.getLong(3).toInt)
              }
            r = r.reverse

            if (r.length >= 6) {
              (0 until 4).foreach(i => db.sql(s"DELETE FROM $table WHERE id=${r(i).trId}"))
              (4 until 6).foreach(i => db.sql(s"UPDATE $table SET count=${r(i).count + 10} WHERE id=${r(i).trId}"))
            }
            (0 until 4).foreach{
              i =>
                db.sql(s"INSERT INTO $table (id,uid,iid,count) VALUES(${(uid << 32) + n},$uid,$n,100)")
                n += 1
            }
        }
      }
      val resSet2 = _dbClient.get.executeQuery(s"SELECT * FROM $table;")
      var count = 0
      while(resSet2.next()) {
        count += 1
      }
      count must_== 8
    }
  }
  "multiple queries" should {
    "normally success" in {
      // TODO: Implement this
      true must_!= true
    }
  }
}

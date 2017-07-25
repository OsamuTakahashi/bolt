package com.sopranoworks.bolt

import com.google.cloud.spanner.TransactionRunner.TransactionCallable
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
  private val _lock = new scala.concurrent.Lock

  override protected def before: Any = {
    _lock.acquire()
    val config = ConfigFactory.load()
    val options = SpannerOptions.newBuilder().build()
    _options = Some(options)
    val spanner = options.getService
    val dbClient = spanner.getDatabaseClient(DatabaseId.of(options.getProjectId, config.getString("spanner.instance"), config.getString("spanner.database")))
    Database.startWith(dbClient)
    _dbClient = Some(dbClient)
    _dbClient.foreach(_.executeQuery("DELETE FROM TEST_TABLE"))
    _dbClient.foreach(_.executeQuery("DELETE FROM TEST_ITEMS"))
  }

  override protected def after: Any = {
    _options.foreach(_.getService.close())
    _options = None
    _dbClient = None
    _lock.release()
  }

  case class Item(trId:Long,userId:Long,itemId:Long,count:Int)

  "INSERT" should {
    "normally success" in {
      import Bolt._

      _dbClient.get.executeQuery("INSERT INTO TEST_TABLE VALUES(103,'test insert');")
      val resSet = _dbClient.get.singleUse().executeQuery(Statement.of("SELECT * FROM TEST_TABLE WHERE id=103"))

      var res = false
      while(resSet.next()) {
        res = resSet.getString("name") == "test insert"
      }
      resSet.close()
      _dbClient.get.executeQuery("DELETE FROM TEST_TABLE WHERE id=103;")

      res must_== true
    }
    "multiple insert" in {
      _dbClient.get.executeQuery("INSERT INTO TEST_ITEMS VALUES(0,0,0,100);")
      _dbClient.get.executeQuery("INSERT INTO TEST_ITEMS VALUES(1,0,1,100);")
      _dbClient.get.executeQuery("INSERT INTO TEST_ITEMS VALUES(2,0,2,100);")
      _dbClient.get.executeQuery("INSERT INTO TEST_ITEMS VALUES(3,0,3,100);")

      val resSet2 = _dbClient.get.singleUse().executeQuery(Statement.of("SELECT * FROM TEST_ITEMS"))

      var count = 0
      while(resSet2.next()) {
        count += 1
      }
      resSet2.close()
      count must_== 4
    }
    "multiple insert 2" in {
      _dbClient.get.executeQuery("INSERT INTO TEST_ITEMS (id,uid,iid,count) VALUES(0,0,0,100);")
      _dbClient.get.executeQuery("INSERT INTO TEST_ITEMS (id,uid,iid,count) VALUES(1,0,1,100);")
      _dbClient.get.executeQuery("INSERT INTO TEST_ITEMS (id,uid,iid,count) VALUES(2,0,2,100);")
      _dbClient.get.executeQuery("INSERT INTO TEST_ITEMS (id,uid,iid,count) VALUES(3,0,3,100);")

      _dbClient.get.executeQuery("INSERT INTO TEST_ITEMS (id,uid,iid,count) VALUES(4,0,4,100)")
      _dbClient.get.executeQuery("INSERT INTO TEST_ITEMS (id,uid,iid,count) VALUES(5,0,5,100)")
      _dbClient.get.executeQuery("INSERT INTO TEST_ITEMS (id,uid,iid,count) VALUES(6,0,6,100)")
      _dbClient.get.executeQuery("INSERT INTO TEST_ITEMS (id,uid,iid,count) VALUES(7,0,7,100)")

      val resSet2 = _dbClient.get.singleUse().executeQuery(Statement.of("SELECT * FROM TEST_ITEMS"))

      var count = 0
      while(resSet2.next()) {
        count += 1
      }
      resSet2.close()
      count must_== 8
    }
    "insert select" in {
      _dbClient.get.executeQuery("INSERT INTO TEST_ITEMS (id,uid,iid,count) VALUES(0,0,0,100);")
      _dbClient.get.executeQuery("INSERT INTO TEST_ITEMS (id,uid,iid,count) VALUES(1,0,1,100);")
      _dbClient.get.executeQuery("INSERT INTO TEST_ITEMS (id,uid,iid,count) VALUES(2,0,2,100);")
      _dbClient.get.executeQuery("INSERT INTO TEST_ITEMS (id,uid,iid,count) VALUES(3,0,3,100);")

      _dbClient.get.executeQuery("INSERT INTO TEST_TABLE SELECT id,uid FROM TEST_ITEMS")

      val resSet2 = _dbClient.get.singleUse().executeQuery(Statement.of("SELECT * FROM TEST_TABLE"))

      var count = 0
      while(resSet2.next()) {
        count += 1
      }
      resSet2.close()
      count must_== 4
    }
  }
  "UPDATE" should {
    "normally success" in {
      _dbClient.get.executeQuery("INSERT INTO TEST_TABLE VALUES(103,'test insert');")
      val resSet = _dbClient.get.singleUse().executeQuery(Statement.of("SELECT * FROM TEST_TABLE WHERE id=103"))

      var res = false
      while(resSet.next()) {
        res = resSet.getString("name") == "test insert"
      }
      resSet.close()
      res must_== true

      _dbClient.get.executeQuery("UPDATE TEST_TABLE SET name='new name' WHERE id=103;")

      val resSet2 = _dbClient.get.singleUse().executeQuery(Statement.of("SELECT * FROM TEST_TABLE WHERE id=103"))

      var res2 = false
      while(resSet2.next()) {
        res2 = resSet2.getString("name") == "new name"
      }
      resSet2.close()

      _dbClient.get.executeQuery("DELETE FROM TEST_TABLE WHERE id=103;")

      res2 must_== true
    }
    "multiple rows" in {
      _dbClient.get.executeQuery("INSERT INTO TEST_TABLE VALUES(101,'user1');")
      _dbClient.get.executeQuery("INSERT INTO TEST_TABLE VALUES(102,'user2');")
      _dbClient.get.executeQuery("INSERT INTO TEST_TABLE VALUES(103,'user3');")

      _dbClient.get.executeQuery("UPDATE TEST_TABLE SET name='new name' WHERE id>100;")

      val resSet2 = _dbClient.get.singleUse().executeQuery(Statement.of("SELECT * FROM TEST_TABLE"))

      var res2 = false
      while(resSet2.next()) {
        res2 = resSet2.getString("name") == "new name"
      }
      resSet2.close()

      _dbClient.get.executeQuery("DELETE FROM TEST_TABLE WHERE id>100;")

      res2 must_== true
    }
    "multiple rows 2" in {
      _dbClient.get.executeQuery("INSERT INTO TEST_TABLE VALUES(101,'user1');")
      _dbClient.get.executeQuery("INSERT INTO TEST_TABLE VALUES(102,'user2');")
      _dbClient.get.executeQuery("INSERT INTO TEST_TABLE VALUES(103,'user3');")

      _dbClient.get.executeQuery("UPDATE TEST_TABLE SET name='new name' WHERE id=101 OR id=103;")

      val resSet2 = _dbClient.get.singleUse().executeQuery(Statement.of("SELECT * FROM TEST_TABLE"))

      var count = 0
      while(resSet2.next()) {
        if (resSet2.getString("name") == "new name")
          count += 1
      }
      resSet2.close()

      _dbClient.get.executeQuery("DELETE FROM TEST_TABLE WHERE id>100;")

      count must_== 2
    }
    "not primary key" in {
      _dbClient.get.executeQuery("INSERT INTO TEST_TABLE VALUES(101,'user1');")
      _dbClient.get.executeQuery("INSERT INTO TEST_TABLE VALUES(102,'user2');")
      _dbClient.get.executeQuery("INSERT INTO TEST_TABLE VALUES(103,'user3');")

      _dbClient.get.executeQuery("UPDATE TEST_TABLE SET name='new name' WHERE name='user1';")

      val resSet2 = _dbClient.get.singleUse().executeQuery(Statement.of("SELECT * FROM TEST_TABLE"))

      var count = 0
      while(resSet2.next()) {
        if (resSet2.getString("name") == "new name")
          count += 1
      }
      resSet2.close()

      _dbClient.get.executeQuery("DELETE FROM TEST_TABLE WHERE id>100;")

      count must_== 1
    }
    "not primary key multi row" in {
      _dbClient.get.executeQuery("INSERT INTO TEST_TABLE VALUES(101,'user1');")
      _dbClient.get.executeQuery("INSERT INTO TEST_TABLE VALUES(102,'user2');")
      _dbClient.get.executeQuery("INSERT INTO TEST_TABLE VALUES(103,'user3');")

      _dbClient.get.executeQuery("UPDATE TEST_TABLE SET name='new name' WHERE name='user1' OR name='user2';")

      val resSet2 = _dbClient.get.singleUse().executeQuery(Statement.of("SELECT * FROM TEST_TABLE"))

      var count = 0
      while(resSet2.next()) {
        if (resSet2.getString("name") == "new name")
          count += 1
      }
      resSet2.close()

      _dbClient.get.executeQuery("DELETE FROM TEST_TABLE WHERE id>100;")

      count must_== 2
    }
  }
  "DELETE" should {
    "multiple rows" in {
      _dbClient.get.executeQuery("INSERT INTO TEST_TABLE VALUES(101,'user1');")
      _dbClient.get.executeQuery("INSERT INTO TEST_TABLE VALUES(102,'user2');")
      _dbClient.get.executeQuery("INSERT INTO TEST_TABLE VALUES(103,'user3');")

      _dbClient.get.executeQuery("DELETE FROM TEST_TABLE WHERE id=101 OR id=104;")

      val resSet2 = _dbClient.get.executeQuery("SELECT * FROM TEST_TABLE")

      var count = 0
      while(resSet2.next()) {
        count += 1
      }
      resSet2.close()
      count must_== 2
    }
  }
  "beginTransaction" should {
    "normally success" in {
      _dbClient.get.executeQuery("INSERT INTO TEST_TABLE VALUES(101,'user1');")
      _dbClient.get.executeQuery("INSERT INTO TEST_TABLE VALUES(102,'user2');")
      _dbClient.get.executeQuery("INSERT INTO TEST_TABLE VALUES(103,'user3');")

      var count0 = 0
      _dbClient.get.beginTransaction {
        db =>
          val resSet = db.executeQuery("SELECT * FROM TEST_TABLE;")
          if (resSet != null) {
            while (resSet.next()) {
              db.executeQuery(s"UPDATE TEST_TABLE set name='new name' WHERE id=${resSet.getLong(0)};")
              count0 += 1
            }
            resSet.close()
          } else {
            println("ResultSet is null!!")
          }
      }
      count0 must_== 3


      val resSet2 = _dbClient.get.executeQuery("SELECT * FROM TEST_TABLE WHERE name='new name';")

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
      resSet2.close()
      count must_== 3
    }
    "multiple commit" in {
      _dbClient.get.beginTransaction {
        db =>
          db.executeQuery("INSERT INTO TEST_ITEMS (id,uid,iid,count) VALUES(0,0,0,100);")
          db.executeQuery("INSERT INTO TEST_ITEMS (id,uid,iid,count) VALUES(1,0,1,100);")
          db.executeQuery("INSERT INTO TEST_ITEMS (id,uid,iid,count) VALUES(2,0,2,100);")
          db.executeQuery("INSERT INTO TEST_ITEMS (id,uid,iid,count) VALUES(3,0,3,100);")
      }

      _dbClient.get.beginTransaction {
        db =>
          val resSet = db.executeQuery(s"SELECT * FROM TEST_ITEMS WHERE uid=0")
          while(resSet.next()) {}
          resSet.close()

          db.executeQuery("INSERT INTO TEST_ITEMS (id,uid,iid,count) VALUES(4,0,4,100)")
          db.executeQuery("INSERT INTO TEST_ITEMS (id,uid,iid,count) VALUES(5,0,5,100)")
          db.executeQuery("INSERT INTO TEST_ITEMS (id,uid,iid,count) VALUES(6,0,6,100)")
          db.executeQuery("INSERT INTO TEST_ITEMS (id,uid,iid,count) VALUES(7,0,7,100)")
      }
      val resSet2 = _dbClient.get.executeQuery("SELECT * FROM TEST_ITEMS;")
      var count = 0
      while(resSet2.next()) {
        count += 1
      }
      resSet2.close()
      count must_== 8
    }
    "multiple commit 2" in {
      val table = "TEST_ITEMS"
      val uid = 0L
      var n = 0
      while(n < 12) {
        _dbClient.get.beginTransaction {
          db =>
            val resSet = db.executeQuery(s"SELECT * FROM $table WHERE uid=$uid")
            var r = List.empty[Item]
            while (resSet.next()) {
                r ::= Item(resSet.getLong(0), resSet.getLong(1), resSet.getLong(2), resSet.getLong(3).toInt)
              }
            r = r.reverse

            if (r.length >= 6) {
              (0 until 4).foreach(i => db.executeQuery(s"DELETE FROM $table WHERE id=${r(i).trId}"))
              (4 until 6).foreach(i => db.executeQuery(s"UPDATE $table SET count=${r(i).count + 10} WHERE id=${r(i).trId}"))
            }
            (0 until 4).foreach{
              i =>
                db.executeQuery(s"INSERT INTO $table (id,uid,iid,count) VALUES(${(uid << 32) + n},$uid,$n,100)")
                n += 1
            }
        }
      }
      val resSet2 = _dbClient.get.executeQuery(s"SELECT * FROM $table;")
      var count = 0
      while(resSet2.next()) {
        count += 1
      }
      resSet2.close()
      count must_== 8
    }
  }
  "tableExists" should {
    "the table exists" in {
      _dbClient.get.tableExists("TEST_TABLE") must_== true
    }
    "the table not exists" in {
      _dbClient.get.tableExists("IMAGINARY_TABLE") must_== false
    }
  }
  "indexExists" should {
    "the index exists" in {
      _dbClient.get.indexExists("TEST_ITEMS","UID_INDEX") must_== true
    }
    "the index not exists" in {
      _dbClient.get.indexExists("ITEST_ITEMS","IMAGINARY_INDEX") must_== false
    }
  }
}

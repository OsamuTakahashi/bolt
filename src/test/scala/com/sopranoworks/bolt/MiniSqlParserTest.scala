/**
  * Bolt
  * MiniSqlParserTest
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt

import java.io.ByteArrayInputStream
import java.util

import com.google.cloud.spanner.{DatabaseClient, ResultSet}
import com.sopranoworks.bolt.Bolt.Nut
import com.sopranoworks.bolt.statements._
import com.sopranoworks.bolt.values._
import org.antlr.v4.runtime.{ANTLRInputStream, BailErrorStrategy, CommonTokenStream}
import org.specs2.mutable.Specification

import scala.collection.JavaConversions._

/**
  * Created by takahashi on 2017/07/17.
  */
class MiniSqlParserTest extends Specification {
  class DummyDatabase extends Database {
    var tables = Map.empty[String,Table]
    override def table(name: String): Option[Table] = tables.get(name)
  }
  
  class DummyNut(dbClient:DatabaseClient = null) extends Nut(dbClient) {
    private val _database = new DummyDatabase
    override def database: Database = _database

    var queryString = ""

    override def executeNativeQuery(sql: String): ResultSet = {
      queryString = sql
      null
    }

    override def executeNativeAdminQuery(admin: Admin, sql: String): Unit = {
      queryString = sql
    }

    override def execute(stmt: NoResult): Unit = {
      stmt match {
        case SimpleInsert(_,_,_,_,values) =>
          queryString = values.map(_.text).mkString(",")
        case InsertSelect(_,_,_,_,subquery) =>
          queryString = subquery.text
        case SimpleUpdate(_,_,_,keysAndValues,_,_) =>
          queryString = keysAndValues.get(0).value.eval.asValue.text
        case Delete(_,_,_,w,_) =>
          queryString = w.whereStmt
      }
    }
  }

  private def _createParser(sql:String) = {
    val source = new ByteArrayInputStream(sql.getBytes("UTF8"))
    val input = new ANTLRInputStream(source)
    val lexer = new MiniSqlLexer(input)
    val tokenStream = new CommonTokenStream(lexer)
    val parser = new MiniSqlParser(tokenStream)
    val nut = new DummyNut()
    parser.setErrorHandler(new BailErrorStrategy())
    parser.nut = nut
    (parser,nut)
  }

  "SELECT" should {
    "SELECT * FROM (SELECT \"apple\" AS fruit, \"carrot\" AS vegetable)" in {
      val sql = "SELECT * FROM (SELECT \"apple\" AS fruit, \"carrot\" AS vegetable)"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT STRUCT(1, 2) FROM Users" in {
      val sql = "SELECT STRUCT(1, 2) FROM Users"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT ARRAY(SELECT STRUCT(1, 2)) FROM Users" in {
      val sql = "SELECT ARRAY(SELECT STRUCT(1, 2)) FROM Users"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM Roster" in {
      val sql = "SELECT * FROM Roster"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM db.Roster" in {
      val sql = "SELECT * FROM db.Roster"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT s.SingerId, s.FirstName, s.LastName, s.SingerInfo\nFROM Singers@{FORCE_INDEX=SingersByFirstLastName} AS s\nWHERE s.FirstName = \"Catalina\" AND s.LastName > \"M\"" in {
      val sql = "SELECT s.SingerId, s.FirstName, s.LastName, s.SingerInfo FROM Singers@{FORCE_INDEX=SingersByFirstLastName} AS s WHERE s.FirstName = \"Catalina\" AND s.LastName > \"M\""
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT s.SingerId, s.FirstName, s.LastName, s.SingerInfo, c.ConcertDate\nFROM Singers@{FORCE_INDEX=SingersByFirstLastName} AS s JOIN\n     Concerts@{FORCE_INDEX=ConcertsBySingerId} AS c ON s.SingerId = c.SingerId\nWHERE s.FirstName = \"Catalina\" AND s.LastName > \"M\"" in {
      val sql = "SELECT s.SingerId, s.FirstName, s.LastName, s.SingerInfo, c.ConcertDate FROM Singers@{FORCE_INDEX=SingersByFirstLastName} AS s JOIN Concerts@{FORCE_INDEX=ConcertsBySingerId} AS c ON s.SingerId = c.SingerId WHERE s.FirstName = \"Catalina\" AND s.LastName > \"M\""
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM T1 t1, t1.array_column" in {
      val sql = "SELECT * FROM T1 t1, t1.array_column"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM T1 t1, t1.struct_column.array_field" in {
      val sql = "SELECT * FROM T1 t1, t1.struct_column.array_field"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT (SELECT ARRAY_AGG(c) FROM t1.array_column c) FROM T1 t1" in {
      val sql = "SELECT (SELECT ARRAY_AGG(c) FROM t1.array_column c) FROM T1 t1"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT a.struct_field1 FROM T1 t1, t1.array_of_structs a" in {
      val sql = "SELECT a.struct_field1 FROM T1 t1, t1.array_of_structs a"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT (SELECT STRING_AGG(a.struct_field1) FROM t1.array_of_structs a) FROM T1 t1" in {
      val sql = "SELECT (SELECT STRING_AGG(a.struct_field1) FROM t1.array_of_structs a) FROM T1 t1"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM UNNEST ([1, 2, 3])" in {
      val sql = "SELECT * FROM UNNEST ([1, 2, 3])"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT x\nFROM mytable AS t,\n  t.struct_typed_column.array_typed_field1 AS x" in {
      val sql = "SELECT x FROM mytable AS t, t.struct_typed_column.array_typed_field1 AS x"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM UNNEST ( ) WITH OFFSET AS num" in {
      val sql = "SELECT * FROM UNNEST ( ) WITH OFFSET AS num"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT AVG ( PointsScored )\nFROM\n( SELECT PointsScored\n  FROM Stats\n  WHERE SchoolID = 77 )" in {
      val sql = "SELECT AVG ( PointsScored ) FROM ( SELECT PointsScored FROM Stats WHERE SchoolID = 77 )"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT r.LastName\nFROM\n( SELECT * FROM Roster) AS r" in {
      val sql = "SELECT r.LastName FROM ( SELECT * FROM Roster) AS r"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT r.LastName, r.SchoolId,\n       ARRAY(SELECT AS STRUCT p.OpponentID, p.PointsScored\n             FROM PlayerStats AS p\n             WHERE p.LastName = r.LastName) AS PlayerStats\nFROM Roster AS r" in {
      val sql = "SELECT r.LastName, r.SchoolId, ARRAY(SELECT AS STRUCT p.OpponentID, p.PointsScored FROM PlayerStats AS p WHERE p.LastName = r.LastName) AS PlayerStats FROM Roster AS r"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM Roster, TeamMascot" in {
      val sql = "SELECT * FROM Roster, TeamMascot"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM Roster CROSS JOIN TeamMascot" in {
      val sql = "SELECT * FROM Roster CROSS JOIN TeamMascot"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM Roster INNER JOIN PlayerStats\nON Roster.LastName = PlayerStats.LastName" in {
      val sql = "SELECT * FROM Roster INNER JOIN PlayerStats ON Roster.LastName = PlayerStats.LastName"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT FirstName\nFROM Roster INNER JOIN PlayerStats\nUSING (LastName)" in {
      val sql = "SELECT FirstName FROM Roster INNER JOIN PlayerStats USING (LastName)"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT FirstName\nFROM Roster INNER JOIN PlayerStats\nON Roster.LastName = PlayerStats.LastName" in {
      val sql = "SELECT FirstName FROM Roster INNER JOIN PlayerStats ON Roster.LastName = PlayerStats.LastName"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM Roster INNER JOIN PlayerStats\nUSING (LastName)" in {
      val sql = "SELECT * FROM Roster INNER JOIN PlayerStats USING (LastName)"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM Roster INNER JOIN PlayerStats\nON Roster.LastName = PlayerStats.LastName" in {
      val sql = "SELECT * FROM Roster INNER JOIN PlayerStats ON Roster.LastName = PlayerStats.LastName"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM a LEFT JOIN b ON TRUE LEFT JOIN c ON TRUE" in {
      val sql = "SELECT * FROM a LEFT JOIN b ON TRUE LEFT JOIN c ON TRUE"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM Roster FULL JOIN TeamMascot USING (SchoolID)\nFULL JOIN PlayerStats USING (LastName)" in {
      val sql = "SELECT * FROM Roster FULL JOIN TeamMascot USING (SchoolID) FULL JOIN PlayerStats USING (LastName)"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM ( (Roster FULL JOIN TeamMascot USING (SchoolID))\nFULL JOIN PlayerStats USING (LastName))" in {
      val sql = "SELECT * FROM ( (Roster FULL JOIN TeamMascot USING (SchoolID)) FULL JOIN PlayerStats USING (LastName))"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM (Roster FULL JOIN (TeamMascot FULL JOIN PlayerStats USING\n(LastName)) USING (SchoolID))" in {
      val sql = "SELECT * FROM (Roster FULL JOIN (TeamMascot FULL JOIN PlayerStats USING (LastName)) USING (SchoolID))"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM a JOIN b ON TRUE, b JOIN c ON TRUE" in {
      val sql = "SELECT * FROM a JOIN b ON TRUE, b JOIN c ON TRUE"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM ((a JOIN b ON TRUE) CROSS JOIN b) JOIN c ON TRUE" in {
      val sql = "SELECT * FROM ((a JOIN b ON TRUE) CROSS JOIN b) JOIN c ON TRUE"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM Roster\nWHERE SchoolID = 52" in {
      val sql = "SELECT * FROM Roster WHERE SchoolID = 52"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM Roster\nWHERE STARTS_WITH(LastName, \"Mc\") OR STARTS_WITH(LastName, \"Mac\")" in {
      val sql = "SELECT * FROM Roster WHERE STARTS_WITH(LastName, \"Mc\") OR STARTS_WITH(LastName, \"Mac\")"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM Roster INNER JOIN TeamMascot\nON Roster.SchoolID = TeamMascot.SchoolID" in {
      val sql = "SELECT * FROM Roster INNER JOIN TeamMascot ON Roster.SchoolID = TeamMascot.SchoolID"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM Roster CROSS JOIN TeamMascot\nWHERE Roster.SchoolID = TeamMascot.SchoolID" in {
      val sql = "SELECT * FROM Roster CROSS JOIN TeamMascot WHERE Roster.SchoolID = TeamMascot.SchoolID"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT SUM(PointsScored), LastName\nFROM PlayerStats\nGROUP BY LastName" in {
      val sql = "SELECT SUM(PointsScored), LastName FROM PlayerStats GROUP BY LastName"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT SUM(PointsScored), LastName, FirstName\nFROM PlayerStats\nGROUP BY LastName, FirstName" in {
      val sql = "SELECT SUM(PointsScored), LastName, FirstName FROM PlayerStats GROUP BY LastName, FirstName"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT SUM(PointsScored), LastName, FirstName\nFROM PlayerStats\nGROUP BY 2, FirstName" in {
      val sql = "SELECT SUM(PointsScored), LastName, FirstName FROM PlayerStats GROUP BY 2, FirstName"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT SUM(PointsScored), LastName as last_name\nFROM PlayerStats\nGROUP BY last_name" in {
      val sql = "SELECT SUM(PointsScored), LastName as last_name FROM PlayerStats GROUP BY last_name"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT LastName\nFROM Roster\nGROUP BY LastName\nHAVING SUM(PointsScored) > 15" in {
      val sql = "SELECT LastName FROM Roster GROUP BY LastName HAVING SUM(PointsScored) > 15"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT LastName, SUM(PointsScored) AS ps\nFROM Roster\nGROUP BY LastName\nHAVING ps > 0" in {
      val sql = "SELECT LastName, SUM(PointsScored) AS ps FROM Roster GROUP BY LastName HAVING ps > 0"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT LastName, SUM(PointsScored) AS total\nFROM PlayerStats\nGROUP BY LastName\nHAVING total > 15" in {
      val sql = "SELECT LastName, SUM(PointsScored) AS total FROM PlayerStats GROUP BY LastName HAVING total > 15"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT LastName\nFROM PlayerStats\nGROUP BY LastName\nHAVING SUM(PointsScored) > 15" in {
      val sql = "SELECT LastName FROM PlayerStats GROUP BY LastName HAVING SUM(PointsScored) > 15"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT LastName, COUNT(*)\nFROM PlayerStats\nGROUP BY LastName\nHAVING SUM(PointsScored) > 15" in {
      val sql = "SELECT LastName, COUNT(*) FROM PlayerStats GROUP BY LastName HAVING SUM(PointsScored) > 15"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT LastName, PointsScored, OpponentID\nFROM PlayerStats\nORDER BY SchoolID, LastName" in {
      val sql = "SELECT LastName, PointsScored, OpponentID FROM PlayerStats ORDER BY SchoolID, LastName"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM Roster\nUNION ALL\nSELECT * FROM TeamMascot\nORDER BY SchoolID" in {
      val sql = "SELECT * FROM Roster UNION ALL SELECT * FROM TeamMascot ORDER BY SchoolID"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "( SELECT * FROM Roster\n  UNION ALL\n  SELECT * FROM TeamMascot )\nORDER BY SchoolID" in {
      val sql = "( SELECT * FROM Roster UNION ALL SELECT * FROM TeamMascot ) ORDER BY SchoolID"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM Roster\nUNION ALL\n( SELECT * FROM TeamMascot\n  ORDER BY SchoolID )" in {
      val sql = "SELECT * FROM Roster UNION ALL ( SELECT * FROM TeamMascot ORDER BY SchoolID )"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT SUM(PointsScored), LastName\nFROM PlayerStats\nORDER BY LastName" in {
      val sql = "SELECT SUM(PointsScored), LastName FROM PlayerStats ORDER BY LastName"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT SUM(PointsScored), LastName\nFROM PlayerStats\nORDER BY 2" in {
      val sql = "SELECT SUM(PointsScored), LastName FROM PlayerStats ORDER BY 2"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT s.FirstName, s2.SongName\nFROM Singers AS s, (SELECT * FROM Songs) AS s2" in {
      val sql = "SELECT s.FirstName, s2.SongName FROM Singers AS s, (SELECT * FROM Songs) AS s2"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT s.FirstName AS name, LOWER(s.FirstName) AS lname\nFROM Singers s" in {
      val sql = "SELECT s.FirstName AS name, LOWER(s.FirstName) AS lname FROM Singers s"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT FirstName\nFROM Singers AS s, s.Concerts" in {
      val sql = "SELECT FirstName FROM Singers AS s, s.Concerts"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT FirstName, s.ReleaseDate\nFROM Singers s WHERE ReleaseDate = 1975" in {
      val sql = "SELECT FirstName, s.ReleaseDate FROM Singers s WHERE ReleaseDate = 1975"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM Singers as s, Songs as s2\nORDER BY s.LastName" in {
      val sql = "SELECT * FROM Singers as s, Songs as s2 ORDER BY s.LastName"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT LastName AS last, SingerID\nFROM Singers\nORDER BY last" in {
      val sql = "SELECT LastName AS last, SingerID FROM Singers ORDER BY last"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT SingerID AS sid, COUNT(Songid) AS s2id\nFROM Songs\nGROUP BY 1\nORDER BY 2 DESC" in {
      val sql = "SELECT SingerID AS sid, COUNT(Songid) AS s2id FROM Songs GROUP BY 1 ORDER BY 2 DESC"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT SingerID AS sid, COUNT(Songid) AS s2id\nFROM Songs\nGROUP BY sid\nORDER BY s2id DESC" in {
      val sql = "SELECT SingerID AS sid, COUNT(Songid) AS s2id FROM Songs GROUP BY sid ORDER BY s2id DESC"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT SingerID FROM Singers, Songs" in {
      val sql = "SELECT SingerID FROM Singers, Songs"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT FirstName AS name, LastName AS name,\nFROM Singers\nGROUP BY name" in {
      val sql = "SELECT FirstName AS name, LastName AS name FROM Singers GROUP BY name"
      val (parser, nut) = _createParser(sql)
      parser.minisql() must throwA[RuntimeException] // duplicate alias
    }
    "SELECT UPPER(LastName) AS LastName\nFROM Singers\nGROUP BY LastName" in {
      val sql = "SELECT UPPER(LastName) AS LastName FROM Singers GROUP BY LastName"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT x, z AS T\nFROM some_table T\nGROUP BY T.x" in {
      // NOTICE: This test will occur error only after resolving references
      val sql = "SELECT x, z AS T FROM some_table T GROUP BY T.x"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT LastName, BirthYear AS BirthYear\nFROM Singers\nGROUP BY BirthYear" in {
      val sql = "SELECT LastName, BirthYear AS BirthYear FROM Singers GROUP BY BirthYear"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM Roster JOIN TeamMascot\nON Roster.SchoolID = TeamMascot.SchoolID" in {
      val sql = "SELECT * FROM Roster JOIN TeamMascot ON Roster.SchoolID = TeamMascot.SchoolID"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM Roster CROSS JOIN TeamMascot\nON Roster.SchoolID = TeamMascot.SchoolID" in {
      val sql = "SELECT * FROM Roster CROSS JOIN TeamMascot ON Roster.SchoolID = TeamMascot.SchoolID"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM Roster FULL JOIN TeamMascot\nON Roster.SchoolID = TeamMascot.SchoolID" in {
      val sql = "SELECT * FROM Roster FULL JOIN TeamMascot ON Roster.SchoolID = TeamMascot.SchoolID"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM Roster LEFT JOIN TeamMascot\nON Roster.SchoolID = TeamMascot.SchoolID" in {
      val sql = "SELECT * FROM Roster LEFT JOIN TeamMascot ON Roster.SchoolID = TeamMascot.SchoolID"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT * FROM Roster RIGHT JOIN TeamMascot\nON Roster.SchoolID = TeamMascot.SchoolID" in {
      val sql = "SELECT * FROM Roster RIGHT JOIN TeamMascot ON Roster.SchoolID = TeamMascot.SchoolID"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT LastName, SUM(PointsScored)\nFROM PlayerStats\nGROUP BY LastName" in {
      val sql = "SELECT LastName, SUM(PointsScored) FROM PlayerStats GROUP BY LastName"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT Mascot AS X, SchoolID AS Y\nFROM TeamMascot\nUNION ALL\nSELECT LastName, PointsScored\nFROM PlayerStats" in {
      val sql = "SELECT Mascot AS X, SchoolID AS Y FROM TeamMascot UNION ALL SELECT LastName, PointsScored FROM PlayerStats"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    // Array
    "SELECT [1, 2, 3] as numbers" in {
      val sql = "SELECT [1, 2, 3] as numbers"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT [\"apple\", \"pear\", \"orange\"] as fruit" in {
      val sql = "SELECT [\"apple\", \"pear\", \"orange\"] as fruit"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT [true, false, true] as booleans" in {
      val sql = "SELECT [true, false, true] as booleans"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT [a, b, c]\nFROM\n  (SELECT 5 AS a,\n          37 AS b,\n          406 AS c)" in {
      val sql = "SELECT [a, b, c] FROM (SELECT 5 AS a, 37 AS b, 406 AS c)"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT [a, b, c]\nFROM\n  (SELECT CAST(5 AS INT64) AS a,\n          CAST(37 AS FLOAT64) AS b,\n          406 AS c)" in {
      val sql = "SELECT [a, b, c] FROM (SELECT CAST(5 AS INT64) AS a, CAST(37 AS FLOAT64) AS b, 406 AS c)"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT ARRAY<FLOAT64>[1, 2, 3] as floats" in {
      val sql = "SELECT ARRAY<FLOAT64>[1, 2, 3] as floats"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT [1, 2, 3] as numbers" in {
      val sql = "SELECT [1, 2, 3] as numbers"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT some_numbers,\n       some_numbers[OFFSET(1)] AS offset_1,\n       some_numbers[ORDINAL(1)] AS ordinal_1\nFROM sequences" in {
      val sql = "SELECT some_numbers, some_numbers[OFFSET(1)] AS offset_1, some_numbers[ORDINAL(1)] AS ordinal_1 FROM sequences"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT some_numbers,\n       ARRAY_LENGTH(some_numbers) AS len\nFROM sequences" in {
      val sql = "SELECT some_numbers, ARRAY_LENGTH(some_numbers) AS len FROM sequences"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT race,\n       participant.name,\n       participant.splits\nFROM\n  (SELECT \"800M\" AS race,\n    [STRUCT(\"Rudisha\" as name, [23.4, 26.3, 26.4, 26.1] AS splits),\n     STRUCT(\"Makhloufi\" AS name, [24.5, 25.4, 26.6, 26.1] AS splits),\n     STRUCT(\"Murphy\" AS name, [23.9, 26.0, 27.0, 26.0] AS splits),\n     STRUCT(\"Bosse\" AS name, [23.6, 26.2, 26.5, 27.1] AS splits),\n     STRUCT(\"Rotich\" AS name, [24.7, 25.6, 26.9, 26.4] AS splits),\n     STRUCT(\"Lewandowski\" AS name, [25.0, 25.7, 26.3, 27.2] AS splits),\n     STRUCT(\"Kipketer\" AS name, [23.2, 26.1, 27.3, 29.4] AS splits),\n     STRUCT(\"Berian\" AS name, [23.7, 26.1, 27.0, 29.3] as splits)]\n     AS participants\n  ) AS r\nCROSS JOIN UNNEST(r.participants) AS participant" in {
      val sql = "SELECT race, participant.name, participant.splits FROM (SELECT \"800M\" AS race, [STRUCT(\"Rudisha\" as name, [23.4, 26.3, 26.4, 26.1] AS splits), STRUCT(\"Makhloufi\" AS name, [24.5, 25.4, 26.6, 26.1] AS splits), STRUCT(\"Murphy\" AS name, [23.9, 26.0, 27.0, 26.0] AS splits), STRUCT(\"Bosse\" AS name, [23.6, 26.2, 26.5, 27.1] AS splits), STRUCT(\"Rotich\" AS name, [24.7, 25.6, 26.9, 26.4] AS splits), STRUCT(\"Lewandowski\" AS name, [25.0, 25.7, 26.3, 27.2] AS splits), STRUCT(\"Kipketer\" AS name, [23.2, 26.1, 27.3, 29.4] AS splits), STRUCT(\"Berian\" AS name, [23.7, 26.1, 27.0, 29.3] as splits)] AS participants) AS r CROSS JOIN UNNEST(r.participants) AS participant"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT race,\n       (SELECT name\n        FROM UNNEST(participants)\n        ORDER BY (\n          SELECT SUM(duration)\n          FROM UNNEST(splits) AS duration) ASC\n          LIMIT 1) AS fastest_racer\nFROM\n  (SELECT \"800M\" AS race,\n    [STRUCT(\"Rudisha\" as name, [23.4, 26.3, 26.4, 26.1] AS splits),\n     STRUCT(\"Makhloufi\" AS name, [24.5, 25.4, 26.6, 26.1] AS splits),\n     STRUCT(\"Murphy\" AS name, [23.9, 26.0, 27.0, 26.0] AS splits),\n     STRUCT(\"Bosse\" AS name, [23.6, 26.2, 26.5, 27.1] AS splits),\n     STRUCT(\"Rotich\" AS name, [24.7, 25.6, 26.9, 26.4] AS splits),\n     STRUCT(\"Lewandowski\" AS name, [25.0, 25.7, 26.3, 27.2] AS splits),\n     STRUCT(\"Kipketer\" AS name, [23.2, 26.1, 27.3, 29.4] AS splits),\n     STRUCT(\"Berian\" AS name, [23.7, 26.1, 27.0, 29.3] as splits)]\n     AS participants\n  ) AS r" in {
      val sql = "SELECT race, (SELECT name FROM UNNEST(participants) ORDER BY ( SELECT SUM(duration) FROM UNNEST(splits) AS duration) ASC LIMIT 1) AS fastest_racer FROM (SELECT \"800M\" AS race, [STRUCT(\"Rudisha\" as name, [23.4, 26.3, 26.4, 26.1] AS splits), STRUCT(\"Makhloufi\" AS name, [24.5, 25.4, 26.6, 26.1] AS splits), STRUCT(\"Murphy\" AS name, [23.9, 26.0, 27.0, 26.0] AS splits), STRUCT(\"Bosse\" AS name, [23.6, 26.2, 26.5, 27.1] AS splits), STRUCT(\"Rotich\" AS name, [24.7, 25.6, 26.9, 26.4] AS splits), STRUCT(\"Lewandowski\" AS name, [25.0, 25.7, 26.3, 27.2] AS splits), STRUCT(\"Kipketer\" AS name, [23.2, 26.1, 27.3, 29.4] AS splits), STRUCT(\"Berian\" AS name, [23.7, 26.1, 27.0, 29.3] as splits)] AS participants) AS r"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT race,\n       (SELECT name\n        FROM UNNEST(participants),\n          UNNEST(splits) AS duration\n        ORDER BY duration ASC LIMIT 1) AS runner_with_fastest_lap\nFROM\n  (SELECT \"800M\" AS race,\n    [STRUCT(\"Rudisha\" as name, [23.4, 26.3, 26.4, 26.1] AS splits),\n     STRUCT(\"Makhloufi\" AS name, [24.5, 25.4, 26.6, 26.1] AS splits),\n     STRUCT(\"Murphy\" AS name, [23.9, 26.0, 27.0, 26.0] AS splits),\n     STRUCT(\"Bosse\" AS name, [23.6, 26.2, 26.5, 27.1] AS splits),\n     STRUCT(\"Rotich\" AS name, [24.7, 25.6, 26.9, 26.4] AS splits),\n     STRUCT(\"Lewandowski\" AS name, [25.0, 25.7, 26.3, 27.2] AS splits),\n     STRUCT(\"Kipketer\" AS name, [23.2, 26.1, 27.3, 29.4] AS splits),\n     STRUCT(\"Berian\" AS name, [23.7, 26.1, 27.0, 29.3] as splits)]\n     AS participants\n  ) AS r" in {
      val sql = "SELECT race, (SELECT name FROM UNNEST(participants), UNNEST(splits) AS duration ORDER BY duration ASC LIMIT 1) AS runner_with_fastest_lap FROM (SELECT \"800M\" AS race, [STRUCT(\"Rudisha\" as name, [23.4, 26.3, 26.4, 26.1] AS splits), STRUCT(\"Makhloufi\" AS name, [24.5, 25.4, 26.6, 26.1] AS splits), STRUCT(\"Murphy\" AS name, [23.9, 26.0, 27.0, 26.0] AS splits), STRUCT(\"Bosse\" AS name, [23.6, 26.2, 26.5, 27.1] AS splits), STRUCT(\"Rotich\" AS name, [24.7, 25.6, 26.9, 26.4] AS splits), STRUCT(\"Lewandowski\" AS name, [25.0, 25.7, 26.3, 27.2] AS splits), STRUCT(\"Kipketer\" AS name, [23.2, 26.1, 27.3, 29.4] AS splits), STRUCT(\"Berian\" AS name, [23.7, 26.1, 27.0, 29.3] as splits)] AS participants) AS r"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT race,\n       (SELECT name\n        FROM UNNEST(participants)\n        CROSS JOIN UNNEST(splits) AS duration\n        ORDER BY duration ASC LIMIT 1) AS runner_with_fastest_lap\nFROM\n  (SELECT \"800M\" AS race,\n    [STRUCT(\"Rudisha\" as name, [23.4, 26.3, 26.4, 26.1] AS splits),\n     STRUCT(\"Makhloufi\" AS name, [24.5, 25.4, 26.6, 26.1] AS splits),\n     STRUCT(\"Murphy\" AS name, [23.9, 26.0, 27.0, 26.0] AS splits),\n     STRUCT(\"Bosse\" AS name, [23.6, 26.2, 26.5, 27.1] AS splits),\n     STRUCT(\"Rotich\" AS name, [24.7, 25.6, 26.9, 26.4] AS splits),\n     STRUCT(\"Lewandowski\" AS name, [25.0, 25.7, 26.3, 27.2] AS splits),\n     STRUCT(\"Kipketer\" AS name, [23.2, 26.1, 27.3, 29.4] AS splits),\n     STRUCT(\"Berian\" AS name, [23.7, 26.1, 27.0, 29.3] as splits)]\n     AS participants\n  ) AS r" in {
      val sql = "SELECT race, (SELECT name FROM UNNEST(participants) CROSS JOIN UNNEST(splits) AS duration ORDER BY duration ASC LIMIT 1) AS runner_with_fastest_lap FROM (SELECT \"800M\" AS race, [STRUCT(\"Rudisha\" as name, [23.4, 26.3, 26.4, 26.1] AS splits), STRUCT(\"Makhloufi\" AS name, [24.5, 25.4, 26.6, 26.1] AS splits), STRUCT(\"Murphy\" AS name, [23.9, 26.0, 27.0, 26.0] AS splits), STRUCT(\"Bosse\" AS name, [23.6, 26.2, 26.5, 27.1] AS splits), STRUCT(\"Rotich\" AS name, [24.7, 25.6, 26.9, 26.4] AS splits), STRUCT(\"Lewandowski\" AS name, [25.0, 25.7, 26.3, 27.2] AS splits), STRUCT(\"Kipketer\" AS name, [23.2, 26.1, 27.3, 29.4] AS splits), STRUCT(\"Berian\" AS name, [23.7, 26.1, 27.0, 29.3] as splits)] AS participants) AS r"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT\n  name, sum(duration) as duration\nFROM\n  (SELECT \"800M\" AS race,\n    [STRUCT(\"Rudisha\" AS name, [23.4, 26.3, 26.4, 26.1] AS splits),\n     STRUCT(\"Makhloufi\" AS name, [24.5, 25.4, 26.6, 26.1] AS splits),\n     STRUCT(\"Murphy\" AS name, [23.9, 26.0, 27.0, 26.0] AS splits),\n     STRUCT(\"Bosse\" AS name, [23.6, 26.2, 26.5, 27.1] AS splits),\n     STRUCT(\"Rotich\" AS name, [24.7, 25.6, 26.9, 26.4] AS splits),\n     STRUCT(\"Lewandowski\" AS name, [25.0, 25.7, 26.3, 27.2] AS splits),\n     STRUCT(\"Kipketer\" AS name, [23.2, 26.1, 27.3, 29.4] AS splits),\n     STRUCT(\"Nathan\" as name, ARRAY<FLOAT64>[] as splits),\n     STRUCT(\"David\" as name, NULL as splits)]\n     AS participants) AS races,\n  races.participants LEFT JOIN participants.splits duration\nGROUP BY name" in {
      val sql = "SELECT name, sum(duration) as duration FROM (SELECT \"800M\" AS race, [STRUCT(\"Rudisha\" AS name, [23.4, 26.3, 26.4, 26.1] AS splits), STRUCT(\"Makhloufi\" AS name, [24.5, 25.4, 26.6, 26.1] AS splits), STRUCT(\"Murphy\" AS name, [23.9, 26.0, 27.0, 26.0] AS splits), STRUCT(\"Bosse\" AS name, [23.6, 26.2, 26.5, 27.1] AS splits), STRUCT(\"Rotich\" AS name, [24.7, 25.6, 26.9, 26.4] AS splits), STRUCT(\"Lewandowski\" AS name, [25.0, 25.7, 26.3, 27.2] AS splits), STRUCT(\"Kipketer\" AS name, [23.2, 26.1, 27.3, 29.4] AS splits), STRUCT(\"Nathan\" as name, ARRAY<FLOAT64>[] as splits), STRUCT(\"David\" as name, NULL as splits)] AS participants) AS races, races.participants LEFT JOIN participants.splits duration GROUP BY name"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT some_numbers,\n  ARRAY(SELECT x * 2\n        FROM UNNEST(some_numbers) AS x) AS doubled\nFROM sequences" in {
      val sql = "SELECT some_numbers, ARRAY(SELECT x * 2 FROM UNNEST(some_numbers) AS x) AS doubled FROM sequences"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT\n  ARRAY(SELECT x * 2\n        FROM UNNEST(some_numbers) AS x\n        WHERE x < 5) AS doubled_less_than_five\nFROM sequences" in {
      val sql = "SELECT ARRAY(SELECT x * 2 FROM UNNEST(some_numbers) AS x WHERE x < 5) AS doubled_less_than_five FROM sequences"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT ARRAY(SELECT DISTINCT x\n             FROM UNNEST(some_numbers) AS x) AS unique_numbers\nFROM sequences\nWHERE id = 1" in {
      val sql = "SELECT ARRAY(SELECT DISTINCT x FROM UNNEST(some_numbers) AS x) AS unique_numbers FROM sequences WHERE id = 1"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT\n   ARRAY(SELECT x\n         FROM UNNEST(some_numbers) AS x\n         WHERE 2 IN UNNEST(some_numbers)) AS contains_two\nFROM sequences" in {
      val sql = "SELECT ARRAY(SELECT x FROM UNNEST(some_numbers) AS x WHERE 2 IN UNNEST(some_numbers)) AS contains_two FROM sequences"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT ARRAY_AGG(fruit) AS fruit_basket\nFROM fruits" in {
      val sql = "SELECT ARRAY_AGG(fruit) AS fruit_basket FROM fruits"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT some_numbers,\n  (SELECT SUM(x)\n   FROM UNNEST(s.some_numbers) x) AS sums\nFROM sequences s" in {
      val sql = "SELECT some_numbers, (SELECT SUM(x) FROM UNNEST(s.some_numbers) x) AS sums FROM sequences s"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT ARRAY(\n  SELECT STRUCT(point)\n  FROM points)\n  AS coordinates" in {
      val sql = "SELECT ARRAY( SELECT STRUCT(point) FROM points) AS coordinates"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    // Information schema
    "SELECT\n  t.table_name\nFROM\n  information_schema.tables AS t\nWHERE\n  t.table_catalog = '' and t.table_schema = ''" in {
      val sql = "SELECT t.table_name FROM information_schema.tables AS t WHERE t.table_catalog = '' and t.table_schema = ''"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT\n  t.table_name,\n  t.parent_table_name\nFROM\n  information_schema.tables AS t\nWHERE\n  t.table_catalog = ''\n  AND\n  t.table_schema = ''\nORDER BY\n  t.table_catalog,\n  t.table_schema,\n  t.table_name" in {
      val sql = "SELECT t.table_name, t.parent_table_name FROM information_schema.tables AS t WHERE t.table_catalog = '' AND t.table_schema = '' ORDER BY t.table_catalog, t.table_schema, t.table_name"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT\n  t.column_name,\n  t.spanner_type,\n  t.is_nullable\nFROM\n  information_schema.columns AS t\nWHERE\n  t.table_catalog = ''\n  AND\n  t.table_schema = ''\n  AND\n  t.table_name = 'MyTable'\nORDER BY\n  t.table_catalog,\n  t.table_schema,\n  t.table_name,\n  t.ordinal_position" in {
      val sql = "SELECT t.column_name, t.spanner_type, t.is_nullable FROM information_schema.columns AS t WHERE t.table_catalog = '' AND t.table_schema = '' AND t.table_name = 'MyTable' ORDER BY t.table_catalog, t.table_schema, t.table_name, t.ordinal_position"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT\n  t.table_name,\n  t.index_name,\n  t.parent_table_name\nFROM\n  information_schema.indexes AS t\nWHERE\n  t.table_catalog = ''\n  AND\n  t.table_schema = ''\n  AND\n  t.index_type != 'PRIMARY_KEY'\nORDER BY\n  t.table_catalog,\n  t.table_schema,\n  t.table_name,\n  t.index_name" in {
      val sql = "SELECT t.table_name, t.index_name, t.parent_table_name FROM information_schema.indexes AS t WHERE t.table_catalog = '' AND t.table_schema = '' AND t.index_type != 'PRIMARY_KEY' ORDER BY t.table_catalog, t.table_schema, t.table_name, t.index_name"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    //
    "SELECT 1+1" in {
      val sql = "SELECT 1+1"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT 1+-1" in {
      val sql = "SELECT 1+-1"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT 1++1" in {
      val sql = "SELECT 1++1"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.INDEX_COLUMNS WHERE TABLE_NAME='ACCOUNT_TABLE' AND INDEX_NAME='DEVICE_ID_INDEX' AND COLUMN_ORDERING IS NULL" in {
      val sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.INDEX_COLUMNS WHERE TABLE_NAME='ACCOUNT_TABLE' AND INDEX_NAME='DEVICE_ID_INDEX' AND COLUMN_ORDERING IS NULL"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT (SELECT * FROM UNNEST([0,2,3])) AS x" in {
      val sql = "SELECT (SELECT * FROM UNNEST([0,2,3])) AS x"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT MAX(x) AS max FROM UNNEST([8, NULL, 37, 4, NULL, 55]) AS x" in {
      val sql = "SELECT MAX(x) AS max FROM UNNEST([8, NULL, 37, 4, NULL, 55]) AS x"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
    "SELECT item_id, SUM(CAST (price AS FLOAT64)), item_type FROM item_table WHERE item_sub_id IS NOT NULL AND (item_count > 0 OR back_order IS NOT NULL) AND item_id IN (101,102,103,104) GROUP BY item_id, item_type" in {
      val sql = "SELECT item_id, SUM(CAST (price AS FLOAT64)), item_type FROM item_table WHERE item_sub_id IS NOT NULL AND (item_count > 0 OR back_order IS NOT NULL) AND item_id IN (101,102,103,104)"// GROUP BY item_id, item_type"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
  }
  "INSERT" should {
    "INSERT INTO test_tbl VALUES(3, IF( (select count from test_tbl where id=2) > 0, 1, 0))" in {
      val sql = "insert into test_tbl values(3, IF( (select count from test_tbl where id=2) > 0, 1, 0))"
      val (parser,nut) = _createParser(sql)
      parser.minisql()
      println(nut.queryString)
      nut.queryString must_== "3,IF((select count from test_tbl where id=2 > 0),1,0)"
    }
    "INSERT INTO TEST_TABLE SELECT x,y FROM UNNEST([0,1,2,3]) AS x, UNNEST(['A','B','C','D']) AS y" in {
      val sql = "SELECT x,y FROM UNNEST([0,1,2,3]) as x, UNNEST(['A','B','C','D']) as y"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
  }
  "DELETE" should {
    "DELETE FROM test_tbl WHERE id > 0" in {
      val sql = "DELETE FROM test_tbl WHERE id > 0"
      val (parser,nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== "WHERE id > 0"

    }
  }
  "array_path" should {
    "OFFSET" in {
      val sql = "UPDATE test_tbl SET count=ARRAY<INT64>[0,1,2][OFFSET(0)] WHERE ID=0"
      val (parser,nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== "0"
    }
    "OFFSET out of range" in {
      val sql = "UPDATE test_tbl SET count=ARRAY<INT64>[0,1,2][OFFSET(3)] WHERE ID=0"
      val (parser,nut) = _createParser(sql)
      parser.minisql() must throwA[RuntimeException]
    }
    "ORDINAL" in {
      val sql = "UPDATE test_tbl SET count=ARRAY<INT64>[0,1,2][ORDINAL(1)] WHERE ID=0"
      val (parser,nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== "0"
    }
    "ORDINAL out of range" in {
      val sql = "UPDATE test_tbl SET count=ARRAY<INT64>[0,1,2][ORDINAL(0)] WHERE ID=0"
      val (parser,nut) = _createParser(sql)
      parser.minisql() must throwA[RuntimeException]
    }
  }
  "escaped string" should {
    "normally success" in {
      val sql = "INSERT INTO TEST_TABLE VALUES(\"\\\"[\\u0085]\\n'\")"
      val (parser,nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== "\"[\u0085]\n'"
    }
  }
  "create" should {
    "create table with string(MAX)" in {
      val sql = "CREATE TABLE TEST_TABLE ( str STRING(MAX) NOT NULL) PRIMARY KEY (str)"
      val (parser, nut) = _createParser(sql)
      parser.minisql()
      nut.queryString must_== sql
    }
  }
}

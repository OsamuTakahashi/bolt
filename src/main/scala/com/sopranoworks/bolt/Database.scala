package com.sopranoworks.bolt

import com.google.cloud.spanner.{DatabaseClient, Statement}

/**
  * Created by takahashi on 2017/03/28.
  */
class Database(dbClient:DatabaseClient) {
  private var _tables = Map.empty[String,Table]

  def table(name:String):Option[Table] = _tables.get(name)

  def loadInformationSchema():Unit = {
    val resSet = dbClient.singleUse().executeQuery(Statement.of(s"SELECT * FROM INFORMATION_SCHEMA.COLUMNS"))
    var tables = Map.empty[String,List[Column]]
    while(resSet.next()) {
      if (resSet.getString("TABLE_SCHEMA") != "INFORMATION_SCHEMA") {
        val t = resSet.getString("TABLE_NAME")
        val col = Column(resSet.getString("COLUMN_NAME"), resSet.getLong("ORDINAL_POSITION").toInt, resSet.getString("SPANNER_TYPE"), resSet.getString("IS_NULLABLE") == "YES")

        tables += t -> (col :: tables.getOrElse(t, Nil))
      }
    }
    var indexes = Map.empty[(String,String),(String,List[IndexColumn])]
    val resSet2 = dbClient.singleUse().executeQuery(Statement.of(s"SELECT * FROM INFORMATION_SCHEMA.INDEX_COLUMNS"))
    while(resSet2.next()) {
      if (resSet2.getString("TABLE_SCHEMA") != "INFORMATION_SCHEMA" && !resSet2.isNull("ORDINAL_POSITION")) {
        val t = resSet2.getString("TABLE_NAME")
        val idx = resSet2.getString("INDEX_NAME")

        val col = IndexColumn(resSet2.getString("COLUMN_NAME"), resSet2.getLong("ORDINAL_POSITION").toInt, resSet2.getString("SPANNER_TYPE"), resSet2.getString("IS_NULLABLE") == "YES", resSet2.getString("COLUMN_ORDERING"))

        indexes += (t, idx) -> (t, col :: indexes.get((t, idx)).map(_._2).getOrElse(Nil))
      }
    }
    var idxForTbl = Map.empty[String,List[Index]]
    indexes.foreach(kv=>idxForTbl += kv._2._1 -> (Index(kv._1._2, kv._2._2.sortWith(_.position < _.position)) :: idxForTbl.getOrElse(kv._2._1,Nil)))

    _tables = tables.map {
      kv=>
        val tbl = kv._1
        val idx = idxForTbl.get(tbl)
        tbl->Table(dbClient,tbl,kv._2.sortWith(_.position < _.position),idx.flatMap(_.find(_.name == "PRIMARY_KEY")).get,idx.map(_.filter(_.name != "PRIMARY_KEY")).getOrElse(List()).map(i=>i.name->i).toMap)
    }
  }
}

object Database {
  private var _databases = Map.empty[DatabaseClient,Database]

  def apply(dbClient:DatabaseClient) = _databases.getOrElse(dbClient,{
    val db = new Database(dbClient)
    db.loadInformationSchema()
    _databases += (dbClient->db)
    db
  })

  def startWith(dbClient:DatabaseClient) = apply(dbClient)
}
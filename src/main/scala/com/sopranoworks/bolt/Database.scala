package com.sopranoworks.bolt

import com.google.cloud.spanner.DatabaseClient

/**
  * Created by takahashi on 2017/03/28.
  */
class Database(dbClient:DatabaseClient) {
  private var _tables = Map.empty[String,Table]

  def table(name:String):Table = _tables.getOrElse(name,{
    val tbl = new Table(dbClient,name)
    _tables += (name->tbl)
    tbl
  })
}

object Database {
  private var _databases = Map.empty[DatabaseClient,Database]

  def apply(dbClient:DatabaseClient) = _databases.getOrElse(dbClient,{
    val db = new Database(dbClient)
    _databases += (dbClient->db)
    db
  })
}
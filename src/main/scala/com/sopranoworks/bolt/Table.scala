package com.sopranoworks.bolt

import com.google.cloud.spanner.{DatabaseClient, Statement}

/**
  * Created by takahashi on 2017/03/28.
  */
class Table(dbClient:DatabaseClient,name:String) {

  private var _columns:Option[List[String]] = None
  private var _columnTypes = Map.empty[String,String]
  private var _key:Option[String] = None

  def key:String =  synchronized {
    _key.getOrElse {
      val resultSet = dbClient.singleUse().executeQuery(Statement.of(s"SELECT * FROM INFORMATION_SCHEMA.INDEX_COLUMNS AS t WHERE t.TABLE_NAME='$name'"))
      var key = ""
      while (_key.isEmpty && resultSet.next()) {
        if (resultSet.getString("INDEX_NAME") == "PRIMARY_KEY") {
          key = resultSet.getString("COLUMN_NAME")
          _key = Some(key)
          resultSet.close()
        }
      }
      key
    }
  }

  def isKey(name:String):Boolean = key == name

  def columnTypeOf(columnName:String):Option[String] = {
    columns
    _columnTypes.get(columnName)
  }

  def columns:List[String] = synchronized {
    _columns.getOrElse {
      val resultSet = dbClient.singleUse().executeQuery(Statement.of(s"SELECT * FROM INFORMATION_SCHEMA.COLUMNS AS t WHERE t.TABLE_NAME='$name'"))
      var columns = List.empty[(String,Int)]

      while(resultSet.next()) {
        val nm = resultSet.getString("COLUMN_NAME")
        val tp = resultSet.getString("SPANNER_TYPE")
        val p = resultSet.getLong("ORDINAL_POSITION").toInt
        columns ::= (nm,p)
        _columnTypes += (nm->tp)
      }
      val col = columns.sortWith(_._2 < _._2).map(_._1)
      _columns = Some(col)
      col
    }
  }


  def column(idx:Int):String = columns(idx)
}

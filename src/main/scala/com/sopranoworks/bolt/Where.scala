package com.sopranoworks.bolt

import com.sopranoworks.bolt.values._

/**
  * Created by takahashi on 2017/03/28.
  */
trait Where {
  def onlyPrimaryKey:Boolean = false
}

case class PrimaryKeyWhere(key:String,value:String) extends Where {
  override def onlyPrimaryKey: Boolean = true
}

case class PrimaryKeyListWhere(key:String,values:java.util.List[Value]) extends Where {
  override def onlyPrimaryKey: Boolean = true
}

case class NormalWhere(whereStmt:String,boolExpression:Value) extends Where
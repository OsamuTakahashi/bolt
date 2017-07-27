/**
  * Bolt
  * Aliases
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt

import com.sopranoworks.bolt.values._

trait Alias {
  def name:String
  def getField(name:String):Option[Alias] = None
}

case class TableAlias(name:String,tableName:String) extends Alias
//case class QueryContextAlias(name:String,context:QueryContext) extends Alias
//case class StructAlias(name:String,struct:StructValue) extends Alias
case class QueryResultAlias(name:String,index:Int,context:QueryContext) extends Alias
case class ExpressionAlias(name:String,value:Value) extends Alias


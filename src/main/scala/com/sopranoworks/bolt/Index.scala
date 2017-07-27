/**
  * Bolt
  * Index
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

/**
  * Created by takahashi on 2017/04/08.
  */
case class Index(name:String,columns:List[IndexColumn]) {
  private implicit def tableColumnValueToString(v:TableColumnValue):String = v.name

  def equalKeys(keys:List[String]):Boolean = {
    keys.length == columns.length && columns.forall(col=>keys.contains(col.name))
  }
  def equalKeys(keys:List[TableColumnValue])(implicit f:TableColumnValue => String):Boolean =
    equalKeys(keys.map(f(_)))

  def asTableColumnValues:List[TableColumnValue] =
    columns.map(col => TableColumnValue(col.name,"",col.position))
}
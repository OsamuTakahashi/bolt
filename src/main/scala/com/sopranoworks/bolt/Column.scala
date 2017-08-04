/**
  * Bolt
  * Column
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt

/**
  * Created by takahashi on 2017/04/08.
  */

/**
  * A class for holding information of Spanner's table column
  * TODO: Modified spannerType's type to Type
  * @param name
  * @param position
  * @param spannerType
  * @param nullable
  */
case class Column(name:String,position:Int,spannerType:String,nullable:Boolean)

/**
  * A class for holding information of Spanner's table index column
  * TODO: Modified spannerType's type to Type
  * @param name
  * @param position
  * @param spannerType
  * @param nullable
  * @param order
  */
case class IndexColumn(name:String,position:Int,spannerType:String,nullable:Boolean,order:String)

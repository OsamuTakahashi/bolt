package com.sopranoworks.bolt

/**
  * Created by takahashi on 2017/04/08.
  */
case class Column(name:String,position:Int,spannerType:String,nullable:Boolean)

case class IndexColumn(name:String,position:Int,spannerType:String,nullable:Boolean,order:String)

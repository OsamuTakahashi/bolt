/**
  * Bolt
  * TableValue
  * TableColumnValue
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt.values

import com.sopranoworks.bolt.QueryContext

/**
  *
  */
case class TableValue(name:String,qc:QueryContext) extends Value {
  override def text: String = name

  override def getField(fieldName: String): Value =
    (for {
      tbl<-qc.nat.database.table(name)
      col<-tbl.columns.find(_.name == fieldName)
    } yield {
      TableColumnValue(fieldName,name,col.position)
    }).getOrElse(throw new RuntimeException(s"Table $name does not have a field named '$fieldName'"))
}


/**
  *
  * @note This value will be only made from IdentifierValud's resolveReference method.
  */
case class TableColumnValue(name:String,tableName:String,index:Int) extends WrappedValue {
  def setValue(v:Value):Unit = _ref = Some(v)

  override def text: String = s"$tableName.$name"
  override def eval: Value =
    _ref match {
      case Some(v) => v
      case None =>
        this
    }

  override def resolveReference(columns: Map[String,TableColumnValue] = Map.empty[String,TableColumnValue]): Map[String,TableColumnValue] = {
    val key = s"$tableName.$name"
    if (!columns.contains(key)) columns + (key->this) else columns
  }


  override def invalidateEvaluatedValueIfContains(values: List[Value]): Boolean = {
    values.contains(this)
  }

  override def asValue: Value = {
    this.eval
    if (_ref.isEmpty) {
      this.resolveReference(Map())
      if (_ref.isEmpty)
        throw new RuntimeException(s"Unresolvable table column identifier $name")
    }
    _ref.get.eval.asValue
  }
}
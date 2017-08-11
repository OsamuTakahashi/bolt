/**
  * Bolt
  * statements/Insert
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt.statements

import com.sopranoworks.bolt.Bolt

trait Insert extends NoResult {
  def nut:Bolt.Nut

  protected def _tableColumns(tableName:String) =
    nut.database.table(tableName) match {
      case Some(tbl) =>
        tbl.columns
      case None =>
        throw new RuntimeException(s"Table not found $tableName")
    }
}

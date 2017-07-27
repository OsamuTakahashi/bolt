/**
  * Bolt
  * DatabaseChangedException
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
  * Created by takahashi on 2017/06/13.
  */
final case class DatabaseChangedException(DatabaseId:String) extends Exception

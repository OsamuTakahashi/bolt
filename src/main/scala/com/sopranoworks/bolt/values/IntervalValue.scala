/**
  * Bolt
  * IntervalValue
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt.values

case class IntervalValue(d:Int,unit:String) extends Value {
  override def text: String = s"INTERVAL $d $unit"
}

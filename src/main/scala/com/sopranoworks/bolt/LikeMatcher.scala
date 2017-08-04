/**
  * Bolt
  * LikeMatcher
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt

import scala.util.matching.Regex

/**
  * A conversion utility from SQL LIKE expression to regular expression
  */
object LikeMatcher {
  def apply(matching:String):Regex = {
    var e = false
    var res = List('^')

    matching.toCharArray.foreach {
      case c@('.' | '*' | '+' | '[' | ']' | '?' | '|' | '^' | '{' | '}' | '(' | ')' | '>' | ':' | '$' ) =>
        res ++= List('\\', c)

      case a if e =>
        res :+= a
        e = false

      case '%' =>
        res ++= List('.', '+')

      case '_' =>
        res :+= '.'

      case '\\' =>
        e = true

      case c =>
        res :+= c
    }
    res :+= '$'
    new String(res.toArray).r
  }
}

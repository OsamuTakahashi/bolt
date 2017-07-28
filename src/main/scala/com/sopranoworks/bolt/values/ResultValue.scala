/**
  * Bolt
  * ResultIndexValue
  * ResultFieldValue
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt.values

/**
  *
  * @param expression
  * @param index
  */
case class ResultIndexValue(expression:Value,index:Int) extends WrappedValue {
  override def text = s"(${expression.text})[$index]"

  override def eval: Value = {
    if (_ref.isEmpty) {
      expression.eval
      _stayUnresolved = expression.stayUnresolved
      if (_stayUnresolved) {
        return this
      }

      _ref = Some(expression.eval.getField(index).eval)
    }
    this
  }
}

/**
  * 
  * @param value
  * @param fieldName
  */
case class ResultFieldValue(value:Value, fieldName:String) extends WrappedValue {
  override def text: String = s"${value.text}.$fieldName"

  override def eval: Value = {
    if (_ref.isEmpty) {
      value.eval
      _stayUnresolved = value.stayUnresolved
      if (_stayUnresolved) {
        return this
      }

      _ref = Some(value.eval.getField(fieldName).eval)
    }
    this
  }
}
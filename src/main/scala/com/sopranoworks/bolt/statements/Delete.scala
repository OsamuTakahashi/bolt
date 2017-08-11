/**
  * Bolt
  * statements/Delete
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt.statements

import com.google.cloud.spanner.{Key, KeySet, Mutation, TransactionContext}
import com.google.cloud.spanner.TransactionRunner.TransactionCallable
import com.sopranoworks.bolt.{Bolt, QueryContext, Where}

import scala.collection.JavaConversions._

case class Delete(nut:Bolt.Nut,qc:QueryContext,tableName:String,where:Where) extends Update {

  def execute():Unit = {
    Option(where) match {
      case Some(Where(_,_,w,_)) =>
        nut.transactionContext match {
          case Some(tr) =>
            if (where.isOptimizedWhere) {
              nut.addMutations(List(where.asDeleteMutation))
            } else {
              val keys = _getTargetKeys(tr, tableName, w)
              if (keys.nonEmpty) {
                val ml = keys.map {
                  k =>
                    Mutation.delete(tableName, Key.of(k: _*))
                }
                nut.addMutations(ml)
              }
            }

          case _ =>
            if (where.isOptimizedWhere) {
              Option(nut.dbClient).foreach(_.write(List(where.asDeleteMutation)))
            } else {
              Option(nut.dbClient).foreach(_.readWriteTransaction()
                .run(new TransactionCallable[Unit] {
                  override def run(transaction: TransactionContext):Unit = {
                      val keys = _getTargetKeys(transaction, tableName, w)
                      if (keys.nonEmpty) {
                        val ml = keys.map {
                          k =>
                            Mutation.delete(tableName, Key.of(k: _*))
                        }
                        transaction.buffer(ml)
                      }
                    }
                  }
                ))}
        }

      case None =>
        val m = Mutation.delete(tableName,KeySet.all())
        Option(nut.dbClient).foreach(_.write(List(m)))
      case _ =>
    }
  }
}

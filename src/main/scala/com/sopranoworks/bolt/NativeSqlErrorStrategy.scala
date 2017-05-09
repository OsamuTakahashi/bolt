package com.sopranoworks.bolt

import org.antlr.v4.runtime.{DefaultErrorStrategy, Parser, RecognitionException}

/**
  * Created by takahashi on 2017/05/09.
  */
class NativeSqlErrorStrategy extends DefaultErrorStrategy {
  override def recover(recognizer: Parser, e: RecognitionException): Unit = {
    super.recover(recognizer, e)

    val stream = recognizer.getTokenStream
    if (stream.LA(1) == MiniSqlParser.SC) {
      val intervalSet = getErrorRecoverySet(recognizer)
      stream.consume()
      consumeUntil(recognizer,intervalSet)
    }
  }
}

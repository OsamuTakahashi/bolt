package com.sopranoworks.bolt

import com.spotify.scio.ContextAndArgs
import org.apache.beam.sdk.io.gcp.spanner._

import scala.collection.JavaConverters._

object DataFlowDumper extends ColumnReader {

  def dump(cmdlineArgs:Array[String],spannerConfig:SpannerConfig,table:Table):Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val numColumns = table.columns.length
    val dataStoreBase = args("output")

    sc
      .customInput(s"${table.name}-input",
        SpannerIO.read()
          .withSpannerConfig(spannerConfig)
          .withTable(table.name)
          .withColumns(table.columns.map(_.name).asJava)
      )
      .map {
        s =>
          (0 until numColumns).map(i => getColumn(s,i).qtext).mkString("(",",",")\n")
      }.saveAsTextFile(s"$dataStoreBase${if (dataStoreBase.endsWith("/")) "" else "/"}${table.name}.spannerdump")

    sc.close()
  }
}

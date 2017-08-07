package com.sopranoworks.bolt

object ParserSetter {
  def set(parser:MiniSqlParser,nut:Bolt.Nut,qc:QueryContext):Unit = {
    parser.nut = nut
    parser.qc = qc
  }
}

package com.lisz.bigdata.sql.parser

import org.antlr.v4.runtime.{ANTLRInputStream, CommonTokenStream}

object TestAntlr {
  def main(args: Array[String]): Unit = {
    val lexer = new aaaLexer(new ANTLRInputStream("{1,{2,3},4}"))
    val tokens = new CommonTokenStream(lexer)
    val parser = new aaaParser(tokens)
    val tree = parser.aaainit()
    println(tree.toStringTree(parser ))
  }

}

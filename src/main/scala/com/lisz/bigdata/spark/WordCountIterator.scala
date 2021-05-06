package com.lisz.bigdata.spark

object WordCountIterator {
  def main(args: Array[String]): Unit = {
    val listStr = List("Hello world", "Hello lisz", "good idea")
    val iter = listStr.iterator
    val iterFlatMap = iter.flatMap(_.split("\\s+"))
    val iterMapList = iterFlatMap.map((_, 1))

  }
}

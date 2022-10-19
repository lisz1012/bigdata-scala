package com.lisz

object Test { // 这个演示不一定成功，由于依赖的问题, 跟spark版本有关，程序本身没啥毛病。😄😄
  def main(args: Array[String]): Unit = {
    val listStr = List("Hello world", "Hello scala", "good idea")
    val listStr2 = listStr.flatMap(_.split("\\s+")).map((_, 1)).groupBy((x) => x._1).map(x => {
      val word = x._1
      val count = x._2.map(t => t._2).sum
      (word, count)
    })
    listStr2.foreach(println)
  }
}
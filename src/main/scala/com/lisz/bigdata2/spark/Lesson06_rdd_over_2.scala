package com.lisz.bigdata2.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD及其应用完结篇，下面转入源码分析，精彩继续😄
 */
object Lesson06_rdd_over_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("topN")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.parallelize(List(
      "hello world",
      "hello spark",
      "hello world",
      "hello world",
      "hello hadoop",
      "hello world",
      "hello lisz"
    ))
    val res = data.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    // 比上面的写法少一次shuffle！key没有发生变化、分区器没有发生变化、分区数没有发生变化。建议用mapValues和flatMapValues，
    // 因为他们会传preservesPartitioning = true不丢弃分区器，不用shuffle
    // map会丢弃前面的分区，导致多余的shuffle
    val res01 = res.mapValues(_ * 10)
    val res02 = res01.groupByKey()
    res02.foreach(println)

    Thread.sleep(1000000)
  }
}

package com.lisz.bigdata2.spark

import org.apache.spark.{SparkConf, SparkContext}

object Lesson05_rdd_advanced {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.parallelize(1 to 10, 5) // 不一定抽10个
//    data.sample(true, 0.1, 222).foreach(println)
//    println("---------------------")
//    data.sample(true, 0.1, 222).foreach(println)
//    println("---------------------")
//    data.sample(false, 0.1, 221).foreach(println)
    println(s"data: ${data.getNumPartitions}")
    val data1 = data.mapPartitionsWithIndex(
      (pi, pt) => {
        pt.map(e => (pi, e))
      }
    )
    //val repartition = data1.repartition(8) // 原来同一个分区的数据尽量去到不同的新分区, 要shuffle了
    val repartition = data1.coalesce(3, true) // false强制不产生shuffle, 而是整个分区移动，实际上是后续的分区连续迭代了前面多个分区的迭代器，类似Flink中的rescale算子
    val res = repartition.mapPartitionsWithIndex(
      (pi, pt) => {
        pt.map(e => (pi, e))
      }
    )
    println(s"data: ${res.getNumPartitions}")
    data1.foreach(println)
    println("--------------------------")
    res.foreach(println)

    Thread.sleep(Long.MaxValue)
  }
}

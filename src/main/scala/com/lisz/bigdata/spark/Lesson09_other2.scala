package com.lisz.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

object Lesson09_other2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("control").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    var n = 0
    val a = sc.longAccumulator("a")   // 分布式统计计算
    val data = sc.parallelize(1 to 10, 2)
    val count : Long = data.map(x => {
      n += 1 // n发送到远程Executor
      if (x % 2 == 0) {
        a.add(1)
      } else {
        a.add(100)
      }
      println(s"executor:n: $n")
      x
    }).count()

    println(s"count: $count")
    println(s"Driver:n: $n")
    println(s"Driver:a: ${a.value}")
    println(s"Driver:avg: ${a.avg}")
    println(s"Driver:sum: ${a.sum}")
    println(s"Driver:count: ${a.count}")
    // 接下来可以根据sum、count的值调整分区数
  }
}

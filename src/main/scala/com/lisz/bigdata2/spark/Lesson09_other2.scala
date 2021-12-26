package com.lisz.bigdata2.spark

import org.apache.spark.{SparkConf, SparkContext}

object Lesson09_other2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("contrl").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.parallelize(1 to 10, 2)

    var n = 0;
    val abc = sc.longAccumulator("abc")

    val count = data.map(x => {
      if (x % 2 == 0) abc.add(1) else abc.add(100)  // 分布式统计计算，Driver端能取回，并能调用avg、sum、count等. 一般附加一些累加器，已决定下面逻辑和算子
      n += 1                     // 发生在Executor端
      println(s"executor: n=$n") // 发生在Executor端
      x
    }).count

    println(s"count $count")
    println(s"Driver: $n") // 输出Driver: 0
    println(s"Driver.abc: ${abc}")  // Driver.abc: LongAccumulator(id: 0, name: Some(abc), value: 505)
    println(s"Driver.abc.avg: ${abc.avg}")  // Driver.abc.avg: 50.5
    println(s"Driver.abc.sum: ${abc.sum}")  // Driver.abc.sum: 505
    println(s"Driver.abc.count: ${abc.count}")  // Driver.abc.count: 10
  }
}

package com.lisz.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

object Lesson07_rdd_control {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("control").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // 贴源的
    val data = sc.parallelize(1 to 10)

    // 转换加工的
    val d2rdd = data.map(x => {
      if (x % 2 == 0) {
        ("A", x)
      } else {
        ("B", x)
      }
    })

    // 调优点：只有那些重复使用的RDD才适合调优：缓存结果数据
    d2rdd.cache()

    // 奇偶分组
    val group = d2rdd.groupByKey()
    group.foreach(println)

    // 奇偶统计
    val kv1 = d2rdd.mapValues(x => 1) // 重复用了d2rdd
    val res = kv1.reduceByKey(_ + _)

    res.foreach(println)

    Thread.sleep(Long.MaxValue)
  }
}

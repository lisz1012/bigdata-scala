package com.lisz.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

object Lesson03_rdd_aggregator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.parallelize(List(
      ("zhang san", 234),
      ("zhang san", 5567),
      ("zhang san", 343),
      ("li si", 212),
      ("li si", 44),
      ("li si", 33),
      ("wang wu", 535),
      ("wang wu", 22)
    ))
    // data is Key-Value pairs
    val group = data.groupByKey()
    group.foreach(println)
    // 行列转换:zhang san的三行经过groupByKey之后转成了value中的三列, 下面再转换回来：
    group.flatMap(e=>e._2.map((e._1,_))).foreach(println)
    println("--------------------------")

    // 下面的也行
    group.flatMap(e => e._2.map(x => (e._1, x)).iterator).foreach(println)
    println("--------------------------")

    // 下面的也行, 跟flatMap一样，是进一条出去多条 scala会帮我们把key拼上各个value里面的数字，对value做聚合，key唯一. e 只是Tuple2中的value, 但这个vallue又可以迭代
    group.flatMapValues(e => e.iterator).foreach(println)
    println("--------------------------")

    //group.mapValues(e => e.toList.sorted.take(2)).flatMapValues(e => e.iterator).foreach(println)
    // 下面的也行，flatMapValues是列转行：一条 key - list 变成多个 key - value
    group.flatMapValues(_.toList.sorted.take(2)).foreach(println)
    println("--------------------------")
  }

}

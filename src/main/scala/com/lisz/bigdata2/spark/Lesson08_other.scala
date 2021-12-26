package com.lisz.bigdata2.spark

import org.apache.spark.{SparkConf, SparkContext}

object Lesson08_other {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("contrl").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.parallelize(List(
      "hello world",
      "hello spark",
      "hi world",
      "hello lisz",
      "hello world",
      "hello hadoop"
    ))
    val data1 = data.flatMap(_.split("\\s+"))
    //val list = List("hello", "world") // 有时候是未知的
    val list = data1.map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false).take(2) // 在集群里发生计算，逻辑被推送出去，结果被回收到Driver端
    //val res = data1.filter(x => list.contains(x)) // 闭包，发生在Driver端，把list闭进去了，序列化，执行发生在Executor。依赖：想闭进去必须实现序列化接口
    val bList = sc.broadcast(list) // 把数据的地址广播出去，下游用的时候来拉取，每个下游RDD只是来拉取一次就可以了。第一次见到是在taskBinary，任务序列化的时候
    val res = data1.filter(x => bList.value.contains(x))
//    list.foreach(println)
    res.foreach(println)
  }
}

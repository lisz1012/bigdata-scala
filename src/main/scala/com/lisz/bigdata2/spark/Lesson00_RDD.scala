package com.lisz.bigdata2.spark

import org.apache.spark.{SparkConf, SparkContext}

object Lesson00_RDD {
  def main(args: Array[String]): Unit = {
    // 交替打印，而不是先打印一堆"map"然后打印一堆"filter"
    // RDD不存数据，存的是计算逻辑！有几个partition就有几个task，task的组装是以递归函数的方式组装：filter(map(textFile))
    val conf = new SparkConf().setMaster("local").setAppName("rdd")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val dataRDD = sc.parallelize(List(1, 2, 3, 4, 5))
    dataRDD.map(x=>{
      println("map" + x)
      x
    }).filter(x=>{
      println("filter" + x)
      true
    }).count()
  }
}

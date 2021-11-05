package com.lisz.bigdata2.spark

import org.apache.spark.{SparkConf, SparkContext}

object Lesson01_rdd_api01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test01")
    val sc = new SparkContext(conf)

    val dataRDD = sc.parallelize(List(1, 2, 3, 4, 5, 4, 3, 2, 1))
    val filterRDD = dataRDD.filter(_ > 3)
    val arr = filterRDD.collect()
    arr.foreach(println)
    println("------------------")
    val dist = dataRDD.distinct().collect()
    dist.foreach(println)
    println("------------------")
    dataRDD.map((_,1)).reduceByKey(_+_).map(_._1).foreach(println)
  }
}

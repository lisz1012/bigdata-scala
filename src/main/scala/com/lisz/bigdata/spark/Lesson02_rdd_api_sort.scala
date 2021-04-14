package com.lisz.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

object Lesson02_rdd_api_sort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sort")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // PV, UV
    //需求：根据数据计算各网站的PV,UV，同时，只显示top5
    //解题：要按PV值，或者UV值排序，取前5名
    val fileRDD = sc.textFile("data/pvuvdata.txt", 5)
    fileRDD.flatMap(_.split("\\s+")).filter(_.startsWith("www")).map((_, 1)).reduceByKey(_+_).map(x=>(x._2,x._1)).sortByKey(false).foreach(println)
    println("-------- PV ----------")
    //fileRDD.map(x=>(x.split("\\s+")(5), 1)).reduceByKey(_+_).map(_.swap).sortByKey(false).map(_.swap).take(5).foreach(println)

    // PV
    //fileRDD.map(x=>(x.split("\\s+")(5), 1)).reduceByKey(_+_).map(_.swap).sortByKey(false).map(_.swap).take(5).foreach(println)
    fileRDD.map(x=>(x.split("\\s+")(5), 1)).reduceByKey(_+_).map(_.swap).sortByKey(false).take(5).map(_.swap).foreach(println)

    println("--------- UV ---------")
    // UV
    //fileRDD.map(x=>((x.split("\\s+")(0), x.split("\\s+")(5)), 1)).distinct().reduceByKey(_+_).map(x=>(x._1._2, x._2)).reduceByKey(_+_).map(_.swap).sortByKey(false).take(5).map(_.swap).foreach(println)
    fileRDD.map(x=>(x.split("\\s+")(5), x.split("\\s+")(0))).distinct().map(x=>(x._1, 1)).reduceByKey(_+_).sortBy(_._2, false).take(5).foreach(println)

    Thread.sleep(Long.MaxValue)
  }
}

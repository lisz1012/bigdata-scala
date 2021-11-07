package com.lisz.bigdata2.spark

import org.apache.spark.{SparkConf, SparkContext}

object Lesson02_rdd_api_sort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sort")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // PV, UV,区别在去重不去重
    // 需求：根据数据计算个网站的PV、UV，只显示top 5
    // 解题：要按照PV、UV值排序取钱5名
    println("-----------PV-----------")
    val fileRDD = sc.textFile("data/pvuvdata.txt", 5)
    fileRDD.map(line => (line.split("\\s+")(5), 1)).reduceByKey(_+_).sortBy(_._2, false).take(5).foreach(println) // sortBy的时候可以(_.swap)
    // fileRDD.map(_.split("\\s+")(5)).map((_, 1)).reduceByKey(_+_).sortBy(_._2, false).top(5).foreach(println)
    println("-----------UV-----------")
    fileRDD.map(line => {
      val strs = line.split("\\s+")
      (strs(5), strs(0))
    }).distinct().map(t=>(t._1, 1)).reduceByKey(_+_).sortBy(_._2, false).take(5).foreach(println)
    // PV和UV做了两次sortBy job，第一次是抽样



    Thread.sleep(Long.MaxValue)












  }
}

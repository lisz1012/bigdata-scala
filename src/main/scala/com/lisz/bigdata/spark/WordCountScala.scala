package com.lisz.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("wordcount")
    conf.setMaster("local")

    val context = new SparkContext(conf)
    //单词统计
    //DATASET
    val fileRDD: RDD[String] = context.textFile("data/testdata.txt")
    //hello world
    val words = fileRDD.flatMap((x: String) => {
      x.split("\\s+")
    })
    //hello
    //world
    val pairWords = words.map((x: String) => (x, 1))
    // (hello, 1)
    // (hello, 1)
    // (world, 1)
    val res = pairWords.reduceByKey((x: Int, y: Int) => x + y)

    res.foreach(println)
    //fileRDD.flatMap(_.split("\\s+")).map((_,1)).reduceByKey(_+_).foreach(println)
    //fileRDD.flatMap(_.split("\\s+")).map((_,1)).reduceByKey(_+_).map((x)=>(x._2,1)).reduceByKey(_+_).foreach(println)

    // RDD can be reused
    val resOver = res.map((x) => (x._2, 1)).reduceByKey(_ + _)
    resOver.foreach(println)

    Thread.sleep(Long.MaxValue)
  }
}

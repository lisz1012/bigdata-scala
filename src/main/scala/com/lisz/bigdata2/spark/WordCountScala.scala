package com.lisz.bigdata2.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("wc")
    conf.setMaster("local") //本地运行

    val sc = new SparkContext(conf)
    sc.setLogLevel("error")
    val fileRDD: RDD[String] = sc.textFile("data/testdata.txt")
    val words: RDD[String] = fileRDD.flatMap(x=>x.split("\\s+"))
    val pairWord = words.map(x=>(x, 1))
    val res = pairWord.reduceByKey(((x, y) => (x + y))) // x是old value， y是value
    res.foreach(println)
    println("---------")


    //sc.textFile("data/testdata.txt").flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_+_).foreach(println)

    // 文中出现n次的单词有几个？
    println("-----------------------")
    //sc.textFile("data/testdata.txt").flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_+_).map(t=>(t._2, 1)).reduceByKey(_+_).foreach(println)
    val resOver = res.map(t => (t._2, 1)).reduceByKey(_ + _)

    res.foreach(println)
    resOver.foreach(println)

    Thread.sleep(Long.MaxValue)
  }
}

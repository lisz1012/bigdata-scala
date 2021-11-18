package com.lisz.bigdata2.spark

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Lesson04_rdd_partitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("partitions")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.parallelize(1 to 10, 2)
    val res01 = data.map(value => {
      println("---------Conn MySQL---------")
      println(s"---------select $value---------")
      println("---------Close MySQL---------")
      value + " selected"
    })
    res01.foreach(println)
    // mapPartitionsWithIndex 拿出了一个分区的索引及其迭代器给了函数体
    data.mapPartitionsWithIndex(
      (pindex, piter) => {
        val lb = new ListBuffer[String] // lb在内存，这是致命的！！spark就是个pipeline模式，迭代器嵌套的模式，数据不会在内存中积压，读一条飞过去一条
        println(s"--$pindex-------Conn MySQL---------")
        while (piter.hasNext) {
          val value = piter.next()
          println(s"---------select $value---------")
          lb += value + "selected"
        }
        println("---------Close MySQL---------")
        lb.iterator
      }
    ).foreach(println)

    println("--------------------------")
    data.mapPartitionsWithIndex(
      (pindex, piter) => {
        new Iterator[String] {
          println(s"-------$pindex----connect---------") //迭代器初始化的时候做连接，只做一次
          override def hasNext = if (piter.hasNext) {
            true
          } else {
            println(s"-------$pindex----close---------") // 没有记录之后再做关闭，做一次
            false
          }

          override def next() = {
            val value = piter.next()
            println(s"---------select $value---------")
            value + " selected"
          }
        }
      }
    ).foreach(println)
  }
}

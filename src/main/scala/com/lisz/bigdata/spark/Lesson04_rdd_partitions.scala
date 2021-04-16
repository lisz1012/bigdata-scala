package com.lisz.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer


object Lesson04_rdd_partitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("partitions")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.parallelize(1 to 10, 2)
    // 外关联，1-10可能都需要走一次SQL查询.Spark算子都要接受函数，而函数传入的时候并不会被立即执行
    val res01 = data.map(
      (value: Int) => {
        println("------- Connect to MySQL -------")
        println(s"----- select $value ------")
        println("------- close connection -------")
        value + " selected"
      })
    res01.foreach(println)

    println("----------------------------")
    data.mapPartitionsWithIndex(
      // map拿了一个分区的迭代器给了函数
      (pindex, piter) => {
        //        println(s"index: $pindex   ${piter.next()}")
        //        piter.map(_+10)
        val lb = new ListBuffer[String] // 致命的，占内存。之前源码发现，spark就是一个pipeline，迭代器嵌套的模式，数据不会在内存积压，迭代器对内存友好
        println(s"-------$pindex Connects to MySQL -------")
        while (piter.hasNext) {
          val value = piter.next()
          println(s"----- select $value  ------")
          lb.+=(value + " selected")
        }
        println("------- close connection -------")
        lb.iterator
      }
    ).foreach(println)
    // 多个分区不能共享connection因为他们的物理位置都不一样
    println("----------------------------")
    data.mapPartitionsWithIndex(
      (pindex, piter) => {
        println(s"-------$pindex Connects to MySQL -------")
        new Iterator[String] {
          override def hasNext: Boolean =
            if (piter.hasNext) true
            else {println("------- close connection -------"); false}

          override def next(): String = {
            val value = piter.next()
            println(s"----- select $value  ------")
            s"----- select $value  ------"
          }
        }
      }
    ).foreach(println)
  }

}

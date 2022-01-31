package com.lisz.bigdata2.spark.streaming

import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Lesson03_DStream_API_2 {
  def main(args: Array[String]): Unit = {
    // low level api。DStream也是属于low级别的
    val conf = new SparkConf().setAppName("asdkfh").setMaster("local[8]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Duration(1000)) // 最小粒度。1000 ms Spark2.x 推荐不要小于100ms，3.x之后1ms已经ok了. 默认 window 1s，slide 1s

    /**
      hello 1
      hi 1
      hi 1

      hello 2
      hi 2
      hi 2
     */
    val dataSource = ssc.socketTextStream("localhost", 8889)
    val format = dataSource.map(_.split(" ")).map(x => (x(0), 1))

    /*
    hello 1
    hi 1
    hi 1

    hello 1
    hi 1
    hi 1
     */
    // 每秒钟看到历史5s的统计结果
//    format.window(Seconds(5), Seconds(1))
    //val res = format.window(Duration(5000), Duration(1000)).reduceByKey(_ + _)
//    val reduce = format.reduceByKey(_ + _)  // 窗口量是1000ms，slide也是1000ms
//    val res = reduce.window(Duration(5000)) // 5秒之后只有导引的任务，而没有进行计算
//    val win = format.window(Duration(5000))  // 先调整量
//    val res = win.reduceByKey(_ + _)         // 再基于上一步的量，整体计算

    // 推荐写法：
    val res = format.reduceByKeyAndWindow(_ + _, Duration(5000))
    res.print



    ssc.start
    ssc.awaitTermination
  }
}

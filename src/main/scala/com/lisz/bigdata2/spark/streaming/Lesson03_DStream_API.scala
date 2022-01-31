package com.lisz.bigdata2.spark.streaming

import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Lesson03_DStream_API {
  def main(args: Array[String]): Unit = {
    // low level api。DStream也是属于low级别的
    val conf = new SparkConf().setAppName("asdkfh").setMaster("local[8]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Duration(1000)) // 最小粒度。1000 ms Spark2.x 推荐不要小于100ms，3.x之后1ms已经ok了

    /**
     * 1。需求：将计算延缓
     * 2。一个数据源要保证有秒级的和5秒级的输出频率
     * 3。而且是在数据输出的时候计算的
     * 数据源是1s中一个hello两个hi
     */
    // 这个数据源的粗力度：1s，来源于StreamContext，后续的处理只能往更大粒度的方向上统计了
    val dataSource = ssc.socketTextStream("localhost", 8889)
    val format = dataSource.map(_.split(" ")).map(x => (x(0), x(1).toInt))
    val res1s1batch = format.reduceByKey(_ + _)
    res1s1batch.mapPartitions(iter=>{println("1s");iter}).print // 打印的频率是1s打印一次

//    val newDS = format.window(Duration(5000))  // 打印频率是1s一次, 一次进来一个新batch出去一个（前五次只进不出），只不过计算量是5个batch
    val newDS = format.window(Duration(5000), Duration(5000))  // 打印频率是5s一次, 计算量是5个batch
    val res5s5batch = newDS.reduceByKey(_ + _)
    res5s5batch.mapPartitions(iter=>{println("5s");iter}).print

    ssc.start
    ssc.awaitTermination
  }
}

package com.lisz.bigdata2.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Lesson01_receiver01 {
  def main(args: Array[String]): Unit = {
    // local[n] 2个就够了，一个个receiver的job，另一个给batch计算的job（只不过如果batch比较大，希望n>2，多出来的线程可以跑并行的job的task，多个线程完成一个job）
    val conf = new SparkConf().setAppName("djhds").setMaster("local[9]")
    val ssc = new StreamingContext(conf, Seconds(5))   // 微批的流式计算，用时间间隔定义批次 (while循环时间间隔触发job，一个job处理一个批次)
    ssc.sparkContext.setLogLevel("ERROR")

    val dataDStream = ssc.socketTextStream("localhost", 8889)
    // hello world
//    val flatDStream = dataDStream.flatMap(_.split(" "))
//    val resDStream = flatDStream.map((_, 1)).reduceByKey(_ + _)
//    resDStream.print()  // 输出算子，不是执行算子
    dataDStream.map(_.split(" ")).map(x=>{
      Thread.sleep(20000) // 如果一个批次处理的时间比较长，他会阻塞后面的批次的执行（看SparkUI的Jobs和Stream选项卡）
      (x(0), x(1))
    }).print

    ssc.start()   // 执行算子，异步

    ssc.awaitTermination()  // 上面是异步的
  }
}

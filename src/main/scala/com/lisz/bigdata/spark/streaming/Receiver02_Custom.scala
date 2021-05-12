package com.lisz.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Receiver02_Custom {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[9]").setAppName("sdfdsf")
    val ssc = new StreamingContext(conf, Seconds(5)) // 微批的流式计算，时间去定义批次（while->时间间隔触发job）
    ssc.sparkContext.setLogLevel("ERROR")

    val dstream = ssc.receiverStream(new CustomReceiver("localhost", 8889))
    dstream.print()


    ssc.start() //执行算子，开始运行。附加一个while(true)循环，根据设置进来的时间间隔，把上面的代码无限循环输出。异步

    ssc.awaitTermination()
  }

}

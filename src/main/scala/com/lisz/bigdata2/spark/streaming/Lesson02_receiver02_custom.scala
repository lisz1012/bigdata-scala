package com.lisz.bigdata2.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Lesson02_receiver02_custom {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sdsa").setMaster("local[9]")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")

    val dstream = ssc.receiverStream(new CustomReceiver("localhost", 8889))
    dstream.print

    ssc.start
    ssc.awaitTermination
  }

}

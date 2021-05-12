package com.lisz.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Receiver01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[9]").setAppName("sdfdsf")
    // local[n] n两个就够了，一个给receiver job的task；另一个给batch计算的job （只不过batch比较大的时候，
    // 则期望n大雨2，多出来的线程可以跑并行的 batch@job@task）
    val ssc = new StreamingContext(conf, Seconds(5)) // 微批的流式计算，时间去定义批次（while->时间间隔触发job）
    ssc.sparkContext.setLogLevel("ERROR")

    val dataDStream = ssc.socketTextStream("192.168.1.102", 8889)
    // hello world
//    val flatDStream = dataDStream.flatMap(_.split("\\s+"))
//    val resDStream = flatDStream.map((_, 1)).reduceByKey(_ + _)
//    resDStream.print() // 输出算子，最终调用了人action算子

    val res = dataDStream.map(_.split("\\s+")).map(vars => {
      Thread.sleep(20000)
      (vars(0), vars(1))
    }
    )
    res.print()


    ssc.start() //执行算子，开始运行。附加一个while(true)循环，根据设置进来的时间间隔，把上面的代码无限循环输出。异步

    ssc.awaitTermination()
  }

}

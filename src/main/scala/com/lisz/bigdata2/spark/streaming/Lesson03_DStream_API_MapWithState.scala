package com.lisz.bigdata2.spark.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, State, StateSpec, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Lesson03_DStream_API_MapWithState {
  def main(args: Array[String]): Unit = {
    // low level api。DStream也是属于low级别的
    val conf = new SparkConf().setAppName("asdkfh").setMaster("local[8]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("hdfs://mycluster/sparktest/checkpoint") // 有状态计算里面必须又这个checkpoint存储状态数据作为聚合结果
    val ssc = new StreamingContext(sc, Duration(1000)) // 最小粒度。1000 ms Spark2.x 推荐不要小于100ms，3.x之后1ms已经ok了，继续向流的方向贴近. 默认 window 1s，slide 1s

    val data: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8889)
    val mappedData: DStream[(String, Int)] = data.map(_.split(" ")).map(x => (x(0), 1))

    val res = mappedData.mapWithState(StateSpec.function(
      (k: String, nv: Option[Int], ov: State[Int]) => { // Seq容易内存溢出，原理同Map, 一般会尽量避免这种积压数据的情况。于是一条条的往State里面去更新，面向同一个key（k）每一条新值更新到老值上.稍微有一点点流式的感觉了，但还是算作批.
        println(s"*****k: $k nv: ${nv.getOrElse(0)} ov: ${ov.getOption().getOrElse(0)} **************")
        (k, nv.getOrElse(0) + ov.getOption().getOrElse(0)) // 返回一个键值对。比mappedData.updateStateByKey((nv: Seq[Int], ov: Option[Int])好的一点是可以对key（k）做判断，然后再决定如何执行
      }
    ))

    res.print


    ssc.start
    ssc.awaitTermination
  }
}

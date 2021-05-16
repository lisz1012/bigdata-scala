package com.lisz.bigdata.spark.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object DStream_API_3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[8]")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("./data/ckp")
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Duration(1000))

    val resource = ssc.socketTextStream("localhost", 8889)
    val format = resource.map(_.split("\\s+")).map(x => (x(0), 1))
    // 调优：用checkpoint并且配合persist或者cache方法
    format.persist(StorageLevel.MEMORY_AND_DISK)
    //reduceByKey 对  combinationBykey的封装  放入函数，聚合函数，combina函数
    val res = format.reduceByKeyAndWindow( // dui reduceByKey要很清楚
      (x: Int, y: Int) => { // 这是个聚合函数和combination函数（两个身份：combineByKey的后两个函数参数）：某一键，batch内有两条的时候才能调到聚合函数身份他，所以只有"hi"会触发这个聚合函数一次，所有键都会以Combine函数的方式调用他一次。spark默认有一个放入函数
        /*
        第二次输出：
        first fun...
        ov: 1    nv: 1     ---> hi的batch内聚合
        first fun...
        ov: 1    nv: 1     ---> hello的batch新数据和历史老数据的comnbine
        first fun...
        ov: 2    nv: 2     ---> hi的batch新数据和历史老数据的comnbine
        first fun...
        ov: 1    nv: 1     ---> ha的batch新数据和历史老数据的comnbine。 注：之后挤出去的batches也要先计算合并再做减法
         */
        println("first fun...")   // 打印次数跟MakeData的数据原始输入一批过来的数据条数有关
        println(s"ov: $x    nv: $y")
        x + y
      },
      (x: Int, y: Int) => {
        println("second fun...")  // 打印此书跟出去的batch 的 reduce之后的数据条数有关
        println(s"ov: $x   oov: $y")
        x - y
      },
      Duration(6000),
      Duration(2000))  // 一次步进几个批次就相当于有几个计算的分区
    res.print()

    ssc.start()
    ssc.awaitTermination()
  }

}

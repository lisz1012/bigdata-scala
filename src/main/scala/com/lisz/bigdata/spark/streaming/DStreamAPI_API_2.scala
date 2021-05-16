package com.lisz.bigdata.spark.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{MapWithStateDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, State, StateSpec, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object DStreamAPI_API_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[8]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("./data/ckp") // 这时候要把resources文件夹中的xml文件都拿走，否则卡住还不报错，因为去hdfs创建目录了
    val ssc = new StreamingContext(sc, Duration(1000)) // 最小力度，batch大小, 统计的时候默认按照批reduceByKey和aggregate的默认范围


    //有状态计算
    //状态<-  历史数据  join、关联  历史的计算要存下来，当前的计算最后还要合到历史数据里
    //  持久化下来 历史的数据状态
    //persist    blockmanager  速度快  可靠性差
    // checkpoin    外界系统   成本高  可靠性好
    //persist  调用后  再做 checkpoin  =>数据会在2个地方都存储
    val data = ssc.socketTextStream("localhost", 8889)

    val mapData = data.map(_.split("\\s+")).map(x => (x(0), 1))
    mapData.persist(StorageLevel.MEMORY_AND_DISK)
//    val res = mapData.updateStateByKey( // 算历史数据 Seq[V], Option[S]) => Option[S] 相当于AOF和RDB
//      (nv: Seq[Int], ov: Option[Int]) => { // Seq可能造成溢出，不推荐了
//        println("...update fun...")
//        val count = nv.count(_ > 0) // Seq里面有多少个当前元素
//        val oldVal = ov.getOrElse(0)
//        Some(count + oldVal)
//      }
//    )
//    res.print()

    //企业中最终用mapWithState做全量有状态统计计算
    val res = mapData.mapWithState(StateSpec.function(
      (k: String, nv: Option[Int], ov: State[Int]) => { // 一条条的更新
        println(s"=***********  k: $k  nv: ${nv.getOrElse(0)}  ov: ${ov.getOption().getOrElse(0)} ***********")
        (k, nv.getOrElse(0) + ov.getOption().getOrElse(0)) // 新值和老值的相加。key还能被检查一下
      }
    ))
    res.print()

    ssc.start()
    ssc.awaitTermination()
  }

}

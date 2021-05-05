package com.lisz.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

object Lesson08_other {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("control").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.parallelize(List(
      "hello world",
      "hello spark",
      "hello world",
      "hello lisz",
      "hello world",
      "hello hadoop"
    ))

    val data1 = data.flatMap(_.split("\\s+"))

    //    val data2 = data1.map((_, 1))
//    val data3 = data2.reduceByKey(_ + _)
//    val data4 = data3.sortBy(x => x._2, false).take(2).map(x => x._1)
//    data4.foreach(println)

    //                                                      _._2 也行
    val list = data1.map((_, 1)).reduceByKey(_ + _).sortBy(x=>x._2, false).keys.take(2) // 推送出去, 再Executor那里执行，然后结果再回收回Driver

    // 第一次见Broadcast是什么时候？ Driver往Executor发送TaskBinary的时候，把RDD序列化成二进制，底层就是用RPC发的
    val blist = sc.broadcast(list) // 在Driver端先要存储下来，类似于cache。无论Driver还是Executor都有SparkEnv。blist只存了一个引用，整整的数据在BlockManager里面，executor用到数据的时候再来拉取，拉过去之后，再跑另外的任务executor就不来拉取了。今后不管再怎么闭包，再用刀blist，数据都一样

    // val list = List("hello", "world")
    val res = data1.filter(x => blist.value.contains(x)) //闭包, Driver端序列化，Executor再反序列化。相比抱紧函数，必须实现了序列化接口。闭包再Driver端发生，执行是要发送到Executor
    res.foreach(println)
    /*
    广播变量也好还是直接闭包数据，都属于垂直join，或者map端join
    map端join可以避免本来应该在本地完成的计算，要通过数据shuffle
    之后，在下游Executor相遇的网络IO。广播小表到大表所在节点，
    减少数据移动，小表一般放在左边。IO是最大的瓶颈，规避IO的水平，
    决定了你的段位
     */
  }
}

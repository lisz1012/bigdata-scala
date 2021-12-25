package com.lisz.bigdata2.spark

import java.util.concurrent.TimeUnit

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Lesson07_rdd_control {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("contrl").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    sc.setCheckpointDir("data/ckp")

    // 贴源的
    val data = sc.parallelize(1 to 10)
    // 转换加工的RDD
    val data2 = data.map(x => {
      if (x % 2 == 0) {
        ("A", x)
      } else {
        ("B", x)
      }
    })
    // 调优点：只有那些重复使用的RDD适合调优，先缓存结果数据，在跑后面的计算
//    data2.cache()
    //data2.persist(StorageLevel.MEMORY_ONLY_SER) // 看似增加了序列化，但是放大到整个集群来看，减少了磁盘IO
    //data2.persist(StorageLevel.MEMORY_AND_DISK)   // 优先放入内存，内存不够了再放磁盘
    data2.persist(StorageLevel.MEMORY_AND_DISK)
    data2.checkpoint() // 利用了上面的 sc.setCheckpointDir("data/ckp")，会触发一个Job。第二个作业
    // 思路上会出现bug

    // 思路再前进一步：权衡调优：可靠性和速度。如果查了很多表才得到的中间数据，来之不易。checkpoint

    //奇偶分组
    val group = data2.groupByKey()
    group.foreach(println)  // 第一个作业

//    val data4 = data2.map(x=>(x._1, 1)).reduceByKey(_ + _)
    val kv1 = data2.mapValues(x => 1)
    val reduce = kv1.reduceByKey(_ + _)
    reduce.foreach(println) // 第三个作业
    val res = reduce.mapValues(x => x + "-lisz")
    res.foreach(println) // 第四个作业

    Thread.sleep(Long.MaxValue)
  }
}

package com.lisz.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object CacheDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("control").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    sc.setCheckpointDir("./data/ckp")  // ("hdfs://mycluster/ooxx/tmp/")

    val data = sc.parallelize(1 to 10)

    val d2rdd = data.map(x => {
      if (x % 2 == 0) {
        ("A", x)
      } else {
        ("B", x)
      }
    })

    // 多几缓存机制
    d2rdd.persist(StorageLevel.MEMORY_AND_DISK) // http://192.168.1.102:4040/storage/ 552 B
    //d2rdd.persist(StorageLevel.MEMORY_ONLY_SER) // http://192.168.1.102:4040/storage/ 338.0 B
    // d2rdd.persist(StorageLevel.MEMORY_AND_DISK) // Store in Memory first, use Disk if there is not enough memory

    d2rdd.checkpoint()

    val group = d2rdd.groupByKey()
    group.foreach(println) // job1

    val kv1 = d2rdd.mapValues(x => 1) // Reuse d2rdd
    val res1 = kv1.reduceByKey(_ + _)

    res1.foreach(println) // job3

    val res2 = res1.mapValues(e => (e + " lisz"))
    res2.foreach(println) // job4

    Thread.sleep(Long.MaxValue)
  }
}

package com.lisz.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

object Lesson_rdd_advanced {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf) //相当于SparkContext相当于 计算层的 Driver，看Driver端的代码的时候，从SparkContext的runJob入手
    sc.setLogLevel("ERROR")

    val data = sc.parallelize(1 to 100, 5)
    // false保证每次抽样不会有重复的元素被抽取出来，不一定给0.1就抽出正好10%, seed一样，每次抽出的也都会一样
//    data.sample(true, 0.1, 222).foreach(println)
//    println("--------------------")
//    data.sample(true, 0.1, 222).foreach(println)
//    println("--------------------")
//    data.sample(false, 0.1, 221).foreach(println)

    println(s"data: ${data.getNumPartitions}")
    // Index是分区号，iterator是分区数据的迭代器
    data.mapPartitionsWithIndex(
      (index, iterator) => {
       iterator.map(x => (index, x))
      }
    ).foreach(println)

    println("----------------------------------------------------")
    // 原本在一个分区的会尽量分开, repartition会产生shuffle
    //val repartition = data.repartition(8)
    val repartition = data.coalesce(8, false) // 如果分区数变，某些合并分区，不散列；如果分区数变大，则不会有任何影响，没办法，因为要强行避免shuffle
    // 分布式情况下，数据移动有两种方式：IO移动和shuffle，前者只需要把所有数据移动到目的地就可以了；后者要计算每一条数据将来的去向
    println(s"data: ${repartition.getNumPartitions}")
    repartition.mapPartitionsWithIndex(
      (index, iterator) => {
        iterator.map((index, _)) //和上面逻辑一样，但是简写了
      }
    )
    .foreach(println)

    Thread.sleep(Long.MaxValue) // 卡在这里，为的是方便查看运行的各个stage情况，这个网页（192.168.1.102:4040）只有运行期间才打得开
  }

}

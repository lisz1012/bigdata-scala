package com.lisz.bigdata2.spark

import org.apache.spark.{SparkConf, SparkContext}

/*
面向数据集的操作：
带函数的非聚合的：map、flatMap
不带函数的：
1。氮元素：union，cartesian 没有函数计算
2。kv元素：cogroup，join 没有函数计算
3。排序
4。聚合计算：reduceByKey 有函数。combinerByKey

cogroup 和 combinerByKey最值钱了

 */
object Lesson01_rdd_api01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test01")
    val sc = new SparkContext(conf)
    sc.setLogLevel("error")

    val dataRDD = sc.parallelize(List(1, 2, 3, 4, 5, 4, 3, 2, 1))
    val filterRDD = dataRDD.filter(_ > 3)
    val arr = filterRDD.collect()
    arr.foreach(println)
    println("------------------")
    val dist = dataRDD.distinct().collect()
    dist.foreach(println)
    println("------------------")
    dataRDD.map((_,1)).reduceByKey(_+_).map(_._1).foreach(println)

    // 交并差集
    println("交并差集")
    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5))
    val rdd2 = sc.parallelize(List(3, 4, 5, 6, 7))
    println(rdd1.partitions.size)  // 打印1
    println(rdd2.partitions.size)  // 打印1
    val unitedRDD = rdd1.union(rdd2)  // 打印2
    println(unitedRDD.partitions.size)
    unitedRDD.foreach(println)
    // union属于Range依赖，属于窄依赖，作用在一台物理机上面，不会产生shuffle
//    println("----------------")
//    val unitedRDD2 = rdd2.union(rdd1)
//    println(unitedRDD2.partitions.size)
//    unitedRDD2.foreach(println)
    // 笛卡尔积，下有需要rdd数据的全部，无需shuffle，及其加工，不用"相同的key为一组"。只有IO拷贝
    // 如果数据不需要区分每一条记录归属于哪一个分区，简洁的，这样的数据不需要partitioner，不需要shuffle
    // shuffle的语义是洗牌，面向每一条记录计算他的分区好，如果有行为，不需要区分记录，本地IO拉取数据，那么这种直接IO一定
    // 比先Partition计算，shuffle落文件，最后再IO拉取速度块！！！spark很人性化，面向数据集提供了不同方法的封装，且方法已经经过经验、
    // 尝试推断出自己的实现方式，人不需要干预（出了一个算子）。一个分区内的数据如果指不定去到下游的哪个分区，那就是shuffle依赖，
    // 中间要有一个分区器。
    val cartesian = rdd1.cartesian(rdd2)
    cartesian.foreach(println)
    println("-----------------")

    // 交集
    println("交集")
    val inters = rdd1.intersection(rdd2)
    inters.foreach(println)

    // 差集.只提供了一个方法，有方向
    println("差集")
    val subtract1 = rdd1.subtract(rdd2)
    subtract1.foreach(println)

    // 关联
    println("关联")
    val kv1 = sc.parallelize(List(
      ("zhangsan", 11),
      ("zhangsan", 12),
      ("lisi", 13),
      ("wangwu", 14)
    ))
    val kv2 = sc.parallelize(List(
      ("zhangsan", 21),
      ("zhangsan", 22),
      ("lisi", 23),
      ("zhaoliu", 28)
    ))
    // cogroup 之所以要shuffle，是由于并不是全量的IO移动数据就能像笛卡尔积一样，全量都被用上。rdd1有2个分区，而rdd2有1个分区，
    // 如果全量移动rdd2的分区数据，会把他的全量数据移动两次，而shuffle只移动了一次
    // cogroup相同的key的value相遇组成一个list
    val cogroup = kv1.cogroup(kv2)
    cogroup.foreach(println)
//    val join = kv1.join(kv2)
//    join.foreach(println)
//    val leftJoin = kv1.leftOuterJoin(kv2)
//    leftJoin.foreach(println)
//    val fullOuterJoin = kv1.fullOuterJoin(kv2)
//    fullOuterJoin.foreach(println)


    while (true) {
      Thread.sleep(10000)
    }

  }
}

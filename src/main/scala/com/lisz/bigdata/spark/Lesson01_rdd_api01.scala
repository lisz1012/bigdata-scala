package com.lisz.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

object Lesson01_rdd_api01 {
  //面向数据集操作：
  //*，带函数的非聚合：  map，flatmap
  //1，单元素：union，cartesion  没有函数计算
  //2，kv元素：cogroup，join   没有函数计算
  //3，排序
  //4，聚合计算  ： reduceByKey  有函数   combinerByKey

  //cogroup
  //combinerByKey

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test01")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val dataRDD = sc.parallelize(List(1, 2, 3, 4, 5, 4, 3, 2, 1))
    val filterRDD = dataRDD.filter(_ > 3)
    val res01 = filterRDD.collect()
    res01.foreach(println)
    println("-------------------")

    val res = dataRDD.map((_, 1)).reduceByKey(_ + _).map(_._1).collect()
    res.foreach(println)

    dataRDD.distinct().foreach(println) // Inside is the same as above line
    //面向数据集开发  面向数据集的API  1，基础API   2，复合API
    //RDD  （HadoopRDD,MappartitionsRDD,ShuffledRDD...）
    //map,flatMap,filter
    //distinct...
    //reduceByKey:  复合  ->  combineByKey（）


    //  面向数据集：  交并差  关联 笛卡尔积

    //面向数据集： 元素 -->  单元素，K,V元素  --> 机构化、非结构化
    println("---------------------------")
    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5))
    val rdd2 = sc.parallelize(List(3, 4, 5, 6, 7))

    //    println(rdd1.partitions.size)
//    println(rdd2.partitions.size)
//    val unionedRDD = rdd1.union(rdd2)
//    println(unionedRDD.partitions.size)
//    unionedRDD.foreach(println)

//    val cartesian = rdd1.cartesian(rdd2)
//    cartesian.foreach(println)
//
//    val intersection = rdd1.intersection(rdd2)
//    intersection.foreach(println)
    //rdd1.union(rdd2).map((_,1)).reduceByKey(_+_).filter(_._2 > 1).map(_._1).foreach(println)

    //rdd1.subtract(rdd2).foreach(println)

    val kv1 = sc.parallelize(List(
      ("zhang san", 11),
      ("zhang san", 12),
      ("li si", 13),
      ("wang wu", 14)
    ))

    val kv2 = sc.parallelize(List(
      ("zhang san", 21),
      ("zhang san", 22),
      ("li si", 23),
      ("zhao liu", 28)
    ))

    val cogroupRDD = kv1.cogroup(kv2)
    cogroupRDD.foreach(println)
    println("----------------------------")
    kv1.join(kv2).foreach(println) // join on key, group by key
    println("----------------------------")
    kv1.leftOuterJoin(kv2).foreach(println)
    println("----------------------------")
    kv1.rightOuterJoin(kv2).foreach(println)
    println("----------------------------")
    kv1.fullOuterJoin(kv2).foreach(println)
    println("----------------------------")

    Thread.sleep(Long.MaxValue)




    //spark很人性，面向数据集提供了不同的方法的封装，且，方法已经经过经验，常识，推算出自己的实现方式
    //人不需要干预（会有一个算子需要干预）
    //    val rdd1: RDD[Int] = sc.parallelize( List( 1,2,3,4,5)  )
    //    val rdd2: RDD[Int] = sc.parallelize( List( 3,4,5,6,7)  )

    //    //差集：提供了一个方法：  有方向的
    //    val subtract: RDD[Int] = rdd1.subtract(rdd2)
    //    subtract.foreach(println)

    //    val intersection: RDD[Int] = rdd1.intersection(rdd2)
    //    intersection.foreach(println)


    //    //  如果数据，不需要区分每一条记录归属于那个分区。。。间接的，这样的数据不需要partitioner。。不需要shuffle
    //    //因为shuffle的语义：洗牌  ---》面向每一条记录计算他的分区号
    //    //如果有行为，不需要区分记录，本地IO拉取数据，那么这种直接IO一定比先Partition。。计算，shuffle落文件，最后再IO拉取的速度快！！！
    //    val cartesian: RDD[(Int, Int)] = rdd1.cartesian(rdd2)
    //
    //    cartesian.foreach(println)


  }

}

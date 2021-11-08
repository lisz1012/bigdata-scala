package com.lisz.bigdata2.spark

import org.apache.spark.{SparkConf, SparkContext}

object Lesson03_rdd_aggregator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.parallelize(List(
      ("zhangsan", 234),
      ("zhangsan", 5667),
      ("zhangsan", 343),
      ("lisi", 212),
      ("lisi", 44),
      ("lisi", 33),
      ("wangwu", 535),
      ("wangwu", 22)
    ))
    // key  value 　-> 一组
    val groupedData = data.groupByKey()
    groupedData.foreach(println)
//    val flatMapData = groupedData.flatMap(x => x._2.map((x._1, _)).iterator) // 不写iterator也行
//    flatMapData.foreach(println)
    val res = groupedData.flatMapValues(_.iterator) //针对value做扁平化，会帮我们把key拼上
    res.foreach(println)

    // mapValues flatMapValues 输入参数是一个value，然后对这个value进行操作，二者不同点是输出一条还是多条数据
    groupedData.mapValues(x => x.toList.sorted.take(2)).foreach(println) // x是一个类似集合的东西：CompactBuffer 一进一出
    groupedData.flatMapValues(x => x.toList.sorted.take(2)).foreach(println) // x是一个类似集合的东西：CompactBuffer 一进多出

    // ReduceByKey给出sum值
    val sum = data.reduceByKey(_ + _)
    val max = data.reduceByKey((ov, nv) => if (ov > nv) ov else nv)
    val min = data.reduceByKey((ov, nv) => if (ov < nv) ov else nv)
    println("-----------总数-----------")
    sum.foreach(println)

    println("----------分组统计个数----------")
    data.groupByKey().flatMapValues(_.iterator).map(x => (x._1, 1)).reduceByKey(_+_).foreach(println)
    val count = data.mapValues(x => 1).reduceByKey(_+_)
    count.foreach(println)
    data.countByKey().foreach(println) //countByKey返回一个Map

    println("-----------平均数-----------")
//    sum.join(count).map(x => (x._1, x._2._1 / x._2._2)).foreach(println)
    sum.join(count).mapValues(x => x._1 / x._2).foreach(println)
    println("-----------平均数改进版-----------")
    data.combineByKey(
      (_, 1),
      (ov:(Int, Int), nv:Int) => {
        (ov._1  + nv, ov._2 + 1)
      },
      (ov:(Int, Int), nv:(Int,Int)) => {
        (ov._1  + nv._1, ov._2 + nv._2)
      }
    ).mapValues(x => x._1 / x._2).foreach(println) // combineByKey和mapValues连用

    Thread.sleep(Long.MaxValue)
  }
}

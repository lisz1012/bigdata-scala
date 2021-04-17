package com.lisz.bigdata.spark

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.Sorting

/*
2019-6-1	39
2019-5-21	33
2019-6-1	38
2019-6-2	31
2018-3-11	18
2018-4-23	22
1970-8-23	23
1970-8-8	32


(2019-6,(2019-6-1,39))
(2019-5,(2019-5-21,33))
(2019-6,(2019-6-1,38))
(2019-6,(2019-6-2,31))
(2018-3,(2018-3-11,18))
(2018-4,(2018-4-23,22))
(1970-8,(1970-8-23,23))
(1970-8,(1970-8-8,32))


(2019-5,CompactBuffer((2019-5-21,33)))
(2018-4,CompactBuffer((2018-4-23,22)))
(2019-6,CompactBuffer((2019-6-1,39), (2019-6-1,38), (2019-6-2,31)))
(2018-3,CompactBuffer((2018-3-11,18)))
(1970-8,CompactBuffer((1970-8-23,23), (1970-8-8,32)))


(2019-5,(2019-5-21,33))
(2018-4,(2018-4-23,22))
(2019-6,(2019-6-1,39))
(2019-6,(2019-6-1,38))
(2018-3,(2018-3-11,18))
(1970-8,(1970-8-8,32))
(1970-8,(1970-8-23,23))

 */
// 生产中是没有这个代码的
object Lesson06_rdd_over {
  def main(args: Array[String]): Unit = {
    // 综合应用算子
    val conf = new SparkConf().setMaster("local").setAppName("temperature")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val fileRDD = sc.textFile("data/tqdata.txt")
    // 自己写的版本 3次shuffle
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    fileRDD.map(x=>(x.split("\\s+")(0), x.split("\\s+")(1))).reduceByKey((ov, nv)=> if (ov > nv) ov else nv) //每一天的最大值
      .map(x=>{
      val strs = x._1.split("\\s+")
      val date = sdf.parse(strs(0))
      calendar.setTime(date)
      (calendar.get(Calendar.YEAR) + "-" + (calendar.get(Calendar.MONTH) + 1),
        (strs(0), Integer.parseInt(x._2)))
    })
      .groupByKey().flatMapValues(_.toList.sortBy(_._2).reverse.take(2))
      .map(_._2).sortByKey()
      .foreach(println)
    println("--------------------------------------------------------")

    // 第一版 1次shuffle，groupByKey和HashMap又有问题了，数据积压在里面，容易OOM
    /*val data = fileRDD.map(line => line.split("[\\s+|-]")).map(arr => (arr(0).toInt, arr(1).toInt, arr(2).toInt, Integer.parseInt(arr(3))))
    val res = data.map(t4 => ((t4._1, t4._2), (t4._3, t4._4))).groupByKey().mapValues(arr => { // groupByKey不建议使用，迭代器要加载元素到哦内存
      val map = new mutable.HashMap[Int, Int]()
      arr.foreach(
        x => {
          if (map.get(x._1).getOrElse(0) < x._2) {
            map.put(x._1, x._2)
          }
        }
      )
      map.toList.sorted(new Ordering[(Int, Int)] {
        override def compare(x: (Int, Int), y: (Int, Int)): Int = y._2.compareTo(x._2)
      })
    })
    res.foreach(println)
     */

    // 第二版 两次shuffle，用了groupByKey。取巧：spark rdd reduceByKey的取mx间接达到去重，让自己的算子变得简单点，shuffle换取上面hashmap的开销
    /*
    val data = fileRDD.map(line => line.split("[\\s+|-]")).map(arr => (arr(0).toInt, arr(1).toInt, arr(2).toInt, Integer.parseInt(arr(3))))
    val deduped = data.map(t => ((t._1, t._2, t._3), t._4)).reduceByKey((ov, nv) => if (ov < nv) nv else ov)
    val res = deduped.map(t => ((t._1._1, t._1._2), (t._1._3, t._2))).groupByKey().mapValues(_.toList.sortBy(_._2).reverse.take(2))
    res.foreach(println)
    */

    /* 三次shuffle 二次排序。用了groupByKey了，取巧，用了spark的RDD的reduceByKey去重，用了sortBy排序，注意：多级shuffle，后续shuffle的key一定是前置RDD key的子集
    val data = fileRDD.map(line => line.split("[\\s+|-]")).map(arr => (arr(0).toInt, arr(1).toInt, arr(2).toInt, Integer.parseInt(arr(3))))
    val sorted = data.sortBy(t => (t._1, t._2, t._4), false) // sortBy只管一步之内，步数多了就可能打乱了
    val deduped = sorted.map(t => ((t._1, t._2, t._3), t._4)).reduceByKey((ov, nv) => if (ov > nv) ov else nv)
    val res = deduped.map(t => ((t._1._1, t._1._2), (t._1._3, t._2))).groupByKey().mapValues(_.toList.sortBy(_._2).reverse.take(2))
    //deduped.map(t=>(t., t._2), (t))groupByKey().mapValues(_.toList.sortBy())
    res.foreach(println)
    */

    // 先排序 两次shuffle。相比上面，没有破坏多极shuffle的key的子集关系
    /*
    val data = fileRDD.map(line => line.split("[\\s+|-]")).map(arr => (arr(0).toInt, arr(1).toInt, arr(2).toInt, Integer.parseInt(arr(3))))
    val sorted = data.sortBy(t => (t._1, t._2, t._4), false) // sortBy只管一步之内，步数多了就可能打乱了
    val res = sorted.map(t => ((t._1, t._2), (t._3, t._4))).groupByKey().mapValues(_.toList.sortBy(_._2).reverse.take(2)) // 但是后面shuffle的key取的是前面key的子集
    res.foreach(println)
    */

    // 分布式计算的核心思想，调优天下无敌，分布式是并行的。离线批量计算有一个特征就是后续步骤（stage）依赖前一步骤。如果前一步骤（stage）能够加上正确的combineByKey。
    // 我们自定义的CombineByKey的函数，是尽量压缩内存中的数据
    val data = fileRDD.map(line => line.split("[\\s+|-]")).map(arr => (arr(0).toInt, arr(1).toInt, arr(2).toInt, Integer.parseInt(arr(3))))
    val kv = data.map(t => ((t._1, t._2), (t._3, t._4)))
    val res = kv.combineByKey( // 前置在了mapper
      (value: (Int, Int)) => Array[(Int, Int)](value, (0, 0)),
      (oldV: Array[(Int, Int)], newV: (Int, Int)) => {
        for (i <- 0 until oldV.length) {
          if (newV._1 == oldV(i)._1) {
            if (newV._2 > oldV(i)._2) {
              oldV(i) = newV
            }
          } else if (newV._2 > oldV(i)._2 && oldV(oldV.length - 1)._1 == 0) {
            oldV(oldV.length - 1) = newV
          } else if (newV._2 <= oldV(i)._2 && oldV(oldV.length - 1)._1 == 0) {
            oldV(oldV.length - 1) = newV
          } else if (newV._2 > oldV(i)._2 && oldV(oldV.length - 1)._1 != 0) {
            oldV(i) = newV
          }
        }
        if (oldV(0)._2 < oldV(1)._2) {
          val tmp = oldV(0)
          oldV(0) = oldV(1)
          oldV(1) = tmp
        }
        oldV
      },
      (v1: Array[(Int, Int)], v2: Array[(Int, Int)]) => {
        var union = v1.union(v2) // union 不严谨，可能会有不同来自mapper的相同的日期
        Sorting.quickSort(union) // 原地排序里面的内容
        union
      }
    )
    res.map(x=>(x._1, x._2.toList)).foreach(println)
    println("--------------------------------------------------------")


    val dataRDD = sc.parallelize(List(
      "hello world",
      "hello spark",
      "hello world",
      "hello hadoop",
      "hello world",
      "hello lisz",
      "hello world"
    ))
    dataRDD.flatMap(_.split("\\s+")).map((_,1)).reduceByKey(_+_).foreach(println)

    println("---------------------让统计结果放大10倍-----------------------------")
    dataRDD.flatMap(_.split("\\s+")).map((_,10)).reduceByKey(_+_).foreach(println)

    Thread.sleep(Long.MaxValue)
  }

}

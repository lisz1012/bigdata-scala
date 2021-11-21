package com.lisz.bigdata2.spark

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Lesson06_rdd_over {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("topN")
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    /**
     * 2019-6-1	39
     * 2019-5-21	33
     * 2019-6-1	38
     * 2019-6-2	31
     * 2018-3-11	18
     * 2018-4-23	22
     * 1970-8-23	23
     * 1970-8-8	32
     */

//    val res = sc.textFile("data/tqdata.txt").map(x => {
//      val split = x.split("\\s+")
//      val split2 = split(0).split("-")
//      (split2(0).toInt, split2(1).toInt, split2(2).toInt, split(1).toInt)
//    }).map(x => {
//      ((x._1, x._2), (x._3, x._4))
//    }).groupByKey().mapValues(arr => {
//      val map = new mutable.HashMap[Int, Int]()
//      arr.foreach(x => {
//        if (map.get(x._1).getOrElse(0) < x._2) {
//          map.put(x._1, x._2)
//        }
//      })
//      map.toList.sorted(new Ordering[(Int, Int)] {
//        override def compare(x: (Int, Int), y: (Int, Int)): Int = {
//          y._2.compareTo(x._2)
//        }
//      }).take(2)
//    })

    implicit val reversed = new Ordering[(Int, Int)] {
      override def compare(x: (Int, Int), y: (Int, Int)): Int = {
        y._2.compareTo(x._2)
      }
    }

//    val res = sc.textFile("data/tqdata.txt").map(x => {
//      val split = x.split("\\s+")
//      val split2 = split(0).split("-")
//      (split2(0).toInt, split2(1).toInt, split2(2).toInt, split(1).toInt)
//    }).map(x=>((x._1, x._2, x._3), x._4)).reduceByKey((x, y) => {
//      if (x > y) x else y
//    }).map(x=>{
//      ((x._1._1, x._1._2), (x._1._3, x._2))
//    }).groupByKey().mapValues(x=>x.toList.sorted.take(2))

//    val res = sc.textFile("data/tqdata.txt").map(x => {
//      val split = x.split("\\s+")
//      val split2 = split(0).split("-")
//      (split2(0).toInt, split2(1).toInt, split2(2).toInt, split(1).toInt)
//    }).sortBy(x => {
//      (x._1, x._2, x._4) // 年、月、温度的倒序
//    }, false).map(x => { // tuple4 变成tuple2
//      ((x._1, x._2, x._3), x._4)
//    }).reduceByKey((x, y) => { // 去重，每一天只保留最高温
//      if (x > y) {
//        x
//      } else {
//        y
//      }
//    }).map(x => {
//      ((x._1._1, x._1._2), (x._1._3, x._2))
//    }).groupByKey()//.mapValues(x=>x.toList.sorted.take(2)) // 后面的算子会扰乱前面的排序


//    val res = sc.textFile("data/tqdata.txt").map(x => {
//      val split = x.split("\\s+")
//      val split2 = split(0).split("-")
//      (split2(0).toInt, split2(1).toInt, split2(2).toInt, split(1).toInt)
//    }).map(x =>((x._1, x._2), (x._3, x._4))).reduceByKey((x, y) => {
//      if (x._2 > y._2) {
//        x
//      } else {
//        y
//      }
//    }).map(x=>{
//      ((x._1._1, x._1._2, x._2._1), x._2._2)
//    }).groupByKey().mapValues(x=>x.toList.sorted.take(2))

//    val data = sc.textFile("data/tqdata.txt").map(x => {
//      val split = x.split("\\s+")
//      val split2 = split(0).split("-")
//      (split2(0).toInt, split2(1).toInt, split2(2).toInt, split(1).toInt)
//    })
//    val sorted = data.sortBy(x => (x._1, x._2, x._4), false)
//    val grouped = sorted.map(x => ((x._1, x._2), (x._3, x._4))).groupByKey() //shuffle是依据前面的key元素的子集，所以顺序不会乱
//    grouped.foreach(println)

    val data = sc.textFile("data/tqdata.txt").map(x => {
    val split = x.split("\\s+")
    val split2 = split(0).split("-")
      (split2(0).toInt, split2(1).toInt, split2(2).toInt, split(1).toInt)
    })
    val grouped = data.map(x => ((x._1, x._2), (x._3, x._4))).combineByKey(
      value => Array(value, (0, 0), (0, 0)), // Array让相邻数据的比较有了空间
      (oldV:Array[(Int, Int)], newV:(Int, Int)) => {
          if (newV._1.equals(oldV(0)._1) && newV._2 > oldV(0)._2) {
            oldV(0) = newV
          } else if (newV._1.equals(oldV(1)._1) && newV._2 > oldV(1)._2) {
            oldV(1) = newV
          } else {
            oldV(2) = newV
          }
        oldV.sorted
      },
      (x:Array[(Int, Int)], y:Array[(Int, Int)]) => {
        x.union(y)
      }
    ) //shuffle是依据前面的key元素的子集，所以顺序不会乱
    grouped.map(x=>(x._1, x._2.toList.sorted.take(2).filter(_._1 != 0))).foreach(println)


    //res.foreach(println)

    Thread.sleep(10000000)
  }

}

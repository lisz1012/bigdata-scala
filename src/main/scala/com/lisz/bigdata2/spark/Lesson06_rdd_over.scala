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

    val res = sc.textFile("data/tqdata.txt").map(x => {
      val split = x.split("\\s+")
      val split2 = split(0).split("-")
      (split2(0).toInt, split2(1).toInt, split2(2).toInt, split(1).toInt)
    }).map(x=>((x._1, x._2, x._3), x._4)).reduceByKey((x, y) => {
      if (x > y) x else y
    }).map(x=>{
      ((x._1._1, x._1._2), (x._1._3, x._2))
    }).groupByKey().mapValues(x=>x.toList.sorted.take(2))


    res.foreach(println)

    Thread.sleep(10000000)
  }

}

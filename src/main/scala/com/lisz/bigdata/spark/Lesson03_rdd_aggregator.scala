package com.lisz.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Lesson03_rdd_aggregator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.parallelize(List(
      ("zhang san", 234),
      ("zhang san", 5567),
      ("zhang san", 343),
      ("li si", 212),
      ("li si", 44),
      ("li si", 33),
      ("wang wu", 535),
      ("wang wu", 22)
    ))
    // data is Key-Value pairs
    val group = data.groupByKey()
    group.foreach(println)
    // 行列转换:zhang san的三行经过groupByKey之后转成了value中的三列, 下面再转换回来：
    group.flatMap(e=>e._2.map((e._1,_))).foreach(println)
    println("--------------------------")

    // 下面的也行
    group.flatMap(e => e._2.map(x => (e._1, x)).iterator).foreach(println)
    println("--------------------------")

    // 下面的也行, 跟flatMap一样，是进一条出去多条 scala会帮我们把key拼上各个value里面的数字，对value做聚合，key唯一. e 只是Tuple2中的value, 但这个vallue又可以迭代
    group.flatMapValues(e => e.iterator).foreach(println)
    println("--------------------------")

    //group.mapValues(e => e.toList.sorted.take(2)).flatMapValues(e => e.iterator).foreach(println)
    // 下面的也行，flatMapValues是列转行：一条 key - list 变成多个 key - value
    group.flatMapValues(_.toList.sorted.take(2)).foreach(println)
    println("--------------------------")

    println("------- sum, count, min, max avg -------------------")
    val sum = data.reduceByKey(_ + _)
    val max = data.reduceByKey((oldValue, newValue) => if (oldValue > newValue) oldValue else newValue)
    val min = data.reduceByKey((oldValue, newValue) => if (oldValue < newValue) oldValue else newValue)
    val count1 = data.map(x => (x._1, 1)).reduceByKey(_+_)
    val count2 = data.mapValues(e => 1).reduceByKey(_ + _)


    println("--------- sum ----------------")
    sum.foreach(println)

    println("--------- max ----------------")
    max.foreach(println)

    println("--------- min ----------------")
    min.foreach(println)

    println("--------- count1 ----------------")
    count1.foreach(println)
    println("--------- count2 ----------------")
    count2.foreach(println)

    println("--------- avg1 -----------------")
    // 👇自己写出来的 avg ^_^
    data.mapValues(x=>(x,1)).reduceByKey((oldVal, newVal)=>(oldVal._1 + newVal._1, oldVal._2 + newVal._2)).mapValues(x=>(x._1 * 1.0 / x._2)).foreach(println)
    println("--------- avg2 -----------------")
    /*
      (zhang san,(6144, 3))
      (wang wu,(557, 2))
      (li si,(289,3))
     */
    // Sean 的方法，利用了之前的结果和join操作
    val tmp: RDD[(String, (Int, Int))] = sum.join(count2)
    tmp.mapValues(x=>(x._1 * 1.0 / x._2)).foreach(println)

    println("--------- avg3 combiner --------")
    val tmpx = data.combineByKey(
      // createCombiner: V => C, 第一条记录的 value 怎么放入 hashmap
      (value: Int) => (value, 1),
      // mergeValue: (C, V) => C,如果有第二条记录，第二条及以后的他们的value怎么放到hashMap里
      (oldValue: (Int, Int), newValue: Int) => (oldValue._1 + newValue, oldValue._2 + 1),
      // mergeCombiners: (C, C) => C 合并溢写结果的函数
      (oldValue: (Int, Int), newValue: (Int, Int)) => (oldValue._1 + newValue._1, oldValue._2 + newValue._2)
    )
    tmpx.mapValues(e => e._1 * 1.0 / e._2).foreach(println)

    Thread.sleep(Long.MaxValue)
  }

}

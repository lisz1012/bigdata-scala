package com.lisz.bigdata.spark.streaming

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object DStream_API {
  def main(args: Array[String]): Unit = {
    // spark streaming 100ms batch -> 1ms
    // Loow level API
    val conf = new SparkConf().setMaster("local[8]").setAppName("testAPI")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Duration(1000)) // 最小粒度，窗口 只能是这个数字的倍数 默认window=1s，slide=1s

    /**
     * 1.需求：将计算延缓
     * 2.一个数据源，要保证，1秒级的数据频率和5秒级的输出频率
     * 3.而且，是在数据输出的时候计算输出时间点的历史数据
     *
     * *.数据源是1s中一个hello 2 个hi
     */

    //这个数据源的粗粒度： 1s  来自于 StreamContext. 后买你的代码只能做更粗粒度的计算，而不能是更细的
    val resource = ssc.socketTextStream("localhost", 8889)

//    val format = resource.map(_.split("\\s+")).map(x => (x(0), x(1).toInt))
//    val res1s1batch = format.reduceByKey(_ + _)
//    res1s1batch.mapPartitions(iter=>{println("1s"); iter}).print() // 打印频率：1s一次
//
//    // window机制中（时间定义大小）1、窗口大小：计算量，取多少batch 2、步进：滑动距离(job启动的间隔/频率)
//    val newDS = format.window(Duration(5000), Duration(5000)) // 每5个batch一算，每次算前5个的，不重不漏。每隔第二个参数秒，取出第一个参数秒的数据进行计算
//    val res5s5batch = newDS.reduceByKey(_ + _)
//    res5s5batch.mapPartitions(iter=>{println("5s"); iter}).print() // 打印频率: 1秒打印一次.


    /*
    hello 1
    hi 1
    hi 1

    hello 2
    hi 2
    hi 2
     */
    val format = resource.map(_.split("\\s+")).map(x => (x(0), x(1).toInt))
    /*
    hello 1
    hi 1
    hi 1

    hello 1
    hi 1
    hi 1
     */

    //-------------------------------------window  api-----------------------------------------------

    /**
     * 总结一下，其实一直有窗口的概念，默认，val ssc = new StreamingContext(sc,Duration(1000))  //最小粒度  约等于：  win：  1000   slide：1000
     */
    // 每秒钟看到过去历史5秒的统计
//    val res = format.window(Duration(5000), Duration(1000)).reduceByKey(_+_) //　 先调整量，在机遇上一步的量上整体进行计算
//    res.print()

//    val reduce = format.reduceByKey(_ + _) // 窗口粒度是1s
//    reduce.window(Duration(5000)).print()


    //format.print()

    //format.reduceByKeyAndWindow((x:Int,y:Int)=>x+y, Duration(5000), Duration(5000)).print() // 俩Duration就不能用_+_了

    /*
     转换到RDD的操作
     两种途径：
     */
//  //transform 中途加工，可以有也可以没有action算子
//    val res = format.transform( // 硬性要求：返回值是RDD
//      (rdd: RDD[(String, Int)]) => {
//        rdd.foreach(println)
//        rdd.map(x=>(x._1, x._2*10))
//    })
//    res.print()

    // 末端处理，要写action算子才能执行
//    format.foreachRDD( // StreamingContext有一个独立的线程执行while (true)循环，在主线程中写的代码是放到执行线程中执行的
//      (rdd) => {
//      rdd.foreach(println) // 可以将println 换成 x=>{}，读写数据库、call webservice等
//    })

    /*
    作用于分为3个级别：Job、RDD、Task
    * RDD是一个单向链表
     * DStream也是一个单向链表
     * 如果把最后一个DStream给SSC
     * 那么ssc可以启动一个独立的线程无while(true){最后一个DStream遍历 ； }

     */
    // 广播变量
    var bc = sc.broadcast((1 to 5).toList)
    //var bc: Broadcast[List[Int]] = null
    var jobNum = 0 // 怎么令jobNum的值随着job的提交执行，递增？
    println("aaaaaaa") // Application，当前主线程执行



//    val res = format.transform( // 每job调用一次
//      rdd => {
//        // 函数是每job级别的
//        println("bbbbbbbb")
//        rdd.map(x => {
//          println("ccccccc") // 每条记录打印一次 task级别
//          x
//        })
//      }
//    )
//    //val res = format.filter(x => bc.value.contains(x._2))
//    res.print()

    val res = format.transform(
      rdd => {
        jobNum += 1  // 每job级别，是在ssc的一个while(true)里面，Driver端执。DStream的函数是在Driver端的，另一个线程里
        println(s"job num: ${jobNum}")
//        if (jobNum <= 5) {
//          bc = sc.broadcast((1 to 5).toList)
//        } else {
//          bc = sc.broadcast((6 to 15).toList)
//        }
          if (jobNum > 5) {
            bc = sc.broadcast((6 to 15).toList)
          }
        rdd.filter(x=>bc.value.contains(x._2)) // 无论多少次Job的执行都是相同的bc，只有RDD接受的函数才是executor端的，才是task级别的
      }
    )
    res.print()


    ssc.start()
    ssc.awaitTermination()
  }

}

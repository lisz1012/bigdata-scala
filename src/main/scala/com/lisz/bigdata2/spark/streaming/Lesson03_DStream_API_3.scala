package com.lisz.bigdata2.spark.streaming

import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Lesson03_DStream_API_3 {
  def main(args: Array[String]): Unit = {
    // low level api。DStream也是属于low级别的
    val conf = new SparkConf().setAppName("asdkfh").setMaster("local[8]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Duration(1000)) // 最小粒度。1000 ms Spark2.x 推荐不要小于100ms，3.x之后1ms已经ok了. 默认 window 1s，slide 1s

    /**
      hello 1
      hi 1
      hi 1

      hello 2
      hi 2
      hi 2
     */
    val dataSource = ssc.socketTextStream("localhost", 8889)
    val format = dataSource.map(_.split(" ")).map(x => (x(0), x(1).toInt))

    /*
      转换到RDD的操作
      有两种途径
      重点是作用域，三个级别：
      application
      job
      rdd：task
      RDD是一个单向链表，DStream也是一个单向链表，如果把最后一个DStream给ssc，那么ssc可以启动一个独立的线程去while(true){最后一个DStream遍历的过程} 而DStream对象里的函数就会随着循环而被多次执行
     */

    val bc = sc.broadcast((1 to 5) toList)
    var jobNum = 0 // 怎么令jobNum随着job的提交执行而递增
    println("aaaa")  // application级别，只打印一次
//    val res = format.filter(x => {
//      bc.value.contains(x._2)
//    })
  val res = format.transform( // 每Job调用一次。 println(bbbb), println(cccc)被包裹在了DStream对象里，而res这个DStream对象在print中被注册到了StreamingContext线程里面去了，那边只要while循环起来就一次次的把bbbb和cccc打印了
    rdd => {
      // 函数是每job级
      println("bbbb") // job级别每秒钟打印一次
      rdd.map(x=>{
        println("cccc") // rdd级别，每条记录x就触发一次（3次）
        x
      })
    }
  )

//    val res = format.transform(rdd => { // DStream包装的就是个RDD
//      rdd.map(x=>(x._1, x._2*10))
//    })

//    format.foreachRDD( // StreamingContext有一个独立的线程执行while(true)循环，主线程中写的代码，被放到执行线程中执行
//      rdd=>{
//        rdd.map(x=>{
//          println("asdasdf")
//          x
//        }).collect()
//        //rdd.foreach(x=>{})
//      }
//    )
    //res.print

//    val res = format.transform( // transform把RDD抠出来，硬性要求返回值是RDD
//      rdd => {
//        rdd.foreach(println) // 可以调起一个action算子，有一个分支执行,RDD可以复用
//        rdd.map(x => (x._1, x._2 * 10)) // 转换，给后面的RDD用
//      }
//    )

    res.print


    ssc.start
    ssc.awaitTermination
  }
}

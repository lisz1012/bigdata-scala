package com.lisz.bigdata2.spark.streaming

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Lesson03_DStream_API_Stateful {
  def main(args: Array[String]): Unit = {
    // low level api。DStream也是属于low级别的
    val conf = new SparkConf().setAppName("asdkfh").setMaster("local[8]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("hdfs://mycluster/sparktest/checkpoint") // 有状态计算里面必须又这个checkpoint存储状态数据作为聚合结果
    val ssc = new StreamingContext(sc, Duration(1000)) // 最小粒度。1000 ms Spark2.x 推荐不要小于100ms，3.x之后1ms已经ok了. 默认 window 1s，slide 1s
    // ssc.checkpoint("hdfs://mycluster/...") // 写入HDFS,还要带配置文件

    /**
      状态：历史数据 join、关联 历史的数据要存下来，当前的计算最后还要合到历史数据里
      Spark原生支持这种又状态的计算，因为他可以持久化历史的数据状态
      persist把数据放到了 blockmanager里，checkpoint把数据放到了外界系统里。前者速度快、可靠性差；后者成本更高，但是可靠性好
      调优：persist之后再做checkpoint：数据会在两个地方都存储，有persist的时候，checkoupoint不用读取源，直接把数据从内存中发送到HDFS
      updateStateByKey里面就包含了persist和checkpoint
     */
    val data: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8889)
    val mappedData: DStream[(String, Int)] = data.map(_.split(" ")).map(x => (x(0), 1))
//    mappedData.reduceByKey(_+_)
    // updateStateByKey中的key只是让历史数据跟当前数据关联起来就可以，不一定是当前数据里的这个key
    val res:DStream[(String, Int)] = mappedData.updateStateByKey((nv: Seq[Int], ov: Option[Int]) => { // 要指明类型和泛型, 每个批次调起两次，因为有两个key。不推荐这么写Seq可能引起内存溢出，改进版见 Lesson03_DStream_API_MapWithState
      println("=== update ===")
      // nv是个集合Seq，ov是历史累计的聚合数据，每个批次的job里面，对着nv求和，最后更新到ov里去，然后输出，有点像combinedByKey
      val count: Int = nv.count(_ > 0) //集合里的每个值只要大于0就计数，而这个计数是当前批次的，需要把它聚合到ov
      val oldVal: Int = ov.getOrElse(0)
      Some(count + oldVal)
    })

    // reduceByKey是对combinedByKey的封装：放入函数、聚合函数、combine函数
    // 第二次输出window=5s，slides=1s
    /*
      first fun.  ov: 1, nv: 1
      first fun.  ov: 1, nv: 1
      first fun.  ov: 2, nv: 2
      (hi,4)
      (hello,2)
      解读：先一个batch(1s的)内部聚合：打印了对于hi执行的聚合函数： first fun.  ov: 1, nv: 1
           然后（hello，1）跟已有的（hello，1）聚合，得到一个 first fun.  ov: 1, nv: 1
           最后，第一次聚合后得到了2个hi，跟第一个batch积累下来的两个hi聚合，打印：first fun.  ov: 2, nv: 2
           mergeValue和mergeCombiner传进来的都是reduceByKeyAndWindow的第一个参数，而只有小batch内第二个相同的key（hi）到来的时候
           才会调起mergeValue而每个key都会跟前面batches积累下来的数据做合并：mergeCombiner
            def combineByKey[C: ClassTag](
              createCombiner: V => C,
              mergeValue: (C, V) => C,
              mergeCombiner: (C, C) => C,
              partitioner: Partitioner,
              mapSideCombine: Boolean = true): DStream[(K, C)] = ssc.withScope {
           还是先做了当前batch内的聚合再做当前batch跟已有的当前窗口（window）历史数据的聚合mergeCombiner
           而mergeValue这个函数还是带有鲜明的"批"的味道，拿微批模拟流！当前batch跟历史积累的数据相当于是在两个不同的分区里，所以
           他们要先分别计算

      第六次打印：
        (hi,10)
        (hello,5)

        first fun.  ov: 1, nv: 1
        second fun.  ov: 10, oov: 2
        second fun.  ov: 5, oov: 1
        first fun.  ov: 8, nv: 2
        first fun.  ov: 4, nv: 1

        second函数被调起了两次，因为一秒钟之内移出去的那一个batch内有两个key，一个从10降了2，另一个从5降了1

      等到移出window的时候，也要先调用first，把该聚合的batches聚合一下（batches之间聚合，而不是其内部相同的key聚合，因为内部的聚合在进入window的时候已经计算过了）
      然后才会计算second函数，把batches之间聚合的结果一并减掉（不是a-b-c而是a-(b+c)）
     */
//    val res = mappedData.reduceByKeyAndWindow((ov:Int, nv:Int) => { // 聚合函数，两条以上才能调用到，计算新进入的数据"加上", 基于单笔记录的。hello不会掉用，只有hi有两条记录，会调用到这个聚合函数
//      println(s"first fun.  ov: $ov, nv: $nv")
//      ov + nv
//    }, (ov:Int, oov:Int)=>{ // 计算挤出去的batch "减法"
//      println(s"second fun.  ov: $ov, oov: $oov")
//      ov - oov
//    }, Duration(6000), Duration(2000))

    res.print


    ssc.start
    ssc.awaitTermination
  }
}

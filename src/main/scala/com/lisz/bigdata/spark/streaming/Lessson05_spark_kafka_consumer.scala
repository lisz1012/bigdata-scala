package com.lisz.bigdata.spark.streaming

import java.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}

object Lessson05_spark_kafka_consumer {
  def main(args: Array[String]): Unit = {
    // 写一个Spark Sreaming的程序
    val conf = new SparkConf().setMaster("local[10]").setAppName("test")
    conf.set("spark.streaming.backpressure.enabled", "true")  // 上次拉太多，处理的太慢，下次就少拉一点
    conf.set("spark.streaming.kafka.maxRatePerPartition", "1")  // 每个分区一次最多可以拿多少条回来
    //conf.set("spark.streaming.backpressure.initialRate", "2")  // 刚刚启动的时候第一次最多可以拿多少条回来，跟上面的同时存在，则会被覆盖
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")  // true，    stop的时候如果手头的还没有处理完，要先处理完灿结束

    val ssc = new StreamingContext(conf, Duration(1000))
    ssc.sparkContext.setLogLevel("ERROR")

    val map = Map[String, Object](
      (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop-02:9092,hadoop-03:9092,hadoop-04:9092"),
      (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer]),
      (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer]),
      (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
      (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"), // 需要手动维护offset：1、kafka 2、第三方
      (ConsumerConfig.GROUP_ID_CONFIG, "test11")
      //(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")  // 不生效，要在conf里set
    )

    // 如何得到Kafka的DSream。
    /*
      刚启动的时候如果是手动向第三方维护offset，要访问第三方数据库取回各Partition的offset，放到第三个参数里
       */
//    val mapSql = Map[TopicPartition, Long](
//      (new TopicPartition("from mysql topic", 0), 33),
//      (new TopicPartition("from mysql topic", 1), 53),
//      (new TopicPartition("from mysql topic", 2), 13)
//    )
    // transaction怎么整是个问题
    val kafka: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent, // Spark的一个Consumer默认对应一个Kafka Partition，consumer运行在spark的executor里，每个consumer只能看到自己的数据
      ConsumerStrategies.Subscribe[String, String](List("topic1"), map)
      //ConsumerStrategies.Subscribe[String, String](List("topic1"), map, mapSql)
    )
//    //通过KafkaUtils得到的第一个DStream要先去转换一下，其实这个DStream就是consumer poll回来的records
//    //将Kafka Record转换成业务逻辑元素：只是提取出key和value
//    val dstream = kafka.map(record => {
//      val topic = record.topic()
//      val partition = record.partition()
//      val offset = record.offset()
//      val key = record.key()
//      val value = record.value()
//
//      (key, (value, topic, partition, offset))
//    })
//    dstream.print()

    // 1。offset从哪里来？2、对着Kafka提交的API从哪里来？
    // 罪魁祸首是第一个通过KafkaUtils创建的DStream。他自己提供的提交API，它内部包含的RDD提供了offset
    // dstream是一个DirectKafkaInputDStream,继承了CanCommitOffsets，这里就有提交的API；而他立马 in 有个RDD，是KafkaRDD，
    // 继承了HasOffsetRanges，这里能得到offset
    val dstream = kafka.map(record => {
      val topic = record.topic()
      val partition = record.partition()
      val offset = record.offset()
      val key = record.key()
      val value = record.value()

      (key, (value, topic, partition, offset))
    })
    dstream.print()
    //完成了业务代码之后
    // 维护offset是为了什么？那个时间点用这个维护的offset？application重启的时候，也就是Driver重启的时候。维护offset的另一个语义是：持久化
    var ranges: Array[OffsetRange]  = null
    // 异步回调，缺陷是有一点点延迟
    kafka.foreachRDD(
      rdd=>{ // rdd是个抽象，假设topic有三个分区，则consumer有三个，则rdd有三个分区。Driver是知道rdd每个分区的offset是从哪到哪的
        println(s"foreachRDD..fun.......")
        ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 闭包, 并通过KafkaUtils得到的第一个DStream向上转型，然后提交offset
        // 1。 维护/持久化offset到kafka
        kafka.asInstanceOf[CanCommitOffsets].commitAsync(ranges, new OffsetCommitCallback {
          override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
            if (offsets != null) {
              ranges.foreach(println)
              println("-------------")
              val iter = offsets.entrySet().iterator()
              while (iter.hasNext) {
                val entry = iter.next()
                println(s"${entry.getKey.partition()} : ${entry.getValue.offset()}")
              }
            }
          }
        })
        // 2。维护/持久化到 MySQL：异步维护、同步维护
        // 同步: 转成RDD编程，拿数据到Driver
        val local = rdd.map(record => {
          (record.key(),
            record.value())
        }).reduceByKey(_ + _).collect()
        /*
        下面要开启事务，在事务中提交数据和offset。之后job再次启动的时候还要再把offset 读回来
         */
      }
    )

    // 正确！将提交offset的代码放在dstream对象的接受的函数里，那么在未来调度线程里，每个job都有机会调用一次这个函数，都有机会提交offset
    // 每次job都会执行 kafka.asInstanceOf[CanCommitOffsets].commitAsync(ranges) 提交offset。rdd=>{}在Driver的while true循环里执行的
//    kafka.foreachRDD(
//      rdd=>{ // rdd是个抽象，假设topic有三个分区，则consumer有三个，则rdd有三个分区。Driver是知道rdd每个分区的offset是从哪到哪的
//        println(s"foreachRDD..fun.......")
//        ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//        // 闭包, 并通过KafkaUtils得到的第一个DStream向上转型，然后提交offset
//        kafka.asInstanceOf[CanCommitOffsets].commitAsync(ranges)
//      }
//    )
    // 下边这种不正确。只会在main函数启动的时候被执行一次。 kafka.asInstanceOf[CanCommitOffsets].commitAsync(ranges) 提交offset
    // 其实没有起到作用
//    kafka.foreachRDD(
//      rdd=>{
//        println(s"foreachRDD..fun.......")
//        ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      }
//    )



    ssc.start()
//    Thread.sleep(3000)
//    kafka.asInstanceOf[CanCommitOffsets].commitAsync(ranges)
    ssc.awaitTermination()
  }

}

package com.lisz.bigdata2.spark.streaming

import java.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.clients.consumer.ConsumerConfig.{AUTO_OFFSET_RESET_CONFIG, ENABLE_AUTO_COMMIT_CONFIG, GROUP_ID_CONFIG, MAX_POLL_RECORDS_CONFIG}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}

object Lesson05_Spark_Kafka_Consumer {
  def main(args: Array[String]): Unit = {
    // spark streaming on Kafka
    val conf = new SparkConf().setMaster("local").setAppName("kafka")
    conf.set("spark.streaming.backpressure.enable", "true") // 这次拉太多下次就接受教训，少拉一点
    conf.set("spark.streaming.kafka.maxRatePerPartition", "2") // 类似于 MAX_POLL_RECORDS_CONFIG，每个分区一次拉多少条, 运行时配置
//    conf.set("spark.streaming.backpressure.initalRate", "2") // 新topic刚刚冷启动的时候拉取多少条
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true") // 新topic刚刚冷启动的时候拉取多少条

    val ssc = new StreamingContext(conf, Duration(1000))
    ssc.sparkContext.setLogLevel("ERROR")

    // 如何得到Kafka的DStream
    val map = Map[String, Object](
      (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop-02:9092"),
      (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer]),
      (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer]),
      (AUTO_OFFSET_RESET_CONFIG, "earliest"),
      (ENABLE_AUTO_COMMIT_CONFIG, "FALSE"),
      (GROUP_ID_CONFIG, "g16"),
      (MAX_POLL_RECORDS_CONFIG, "1")
    )
    val kafka: InputDStream[ConsumerRecord[String, String]]= KafkaUtils.createDirectStream(
      ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](List("topic1"), map)
    )
    // 通过kafkaUtils得到的第一个DStream要先去转换一下，其实这个就是consumer poll回来的records，从kafka记录转换成业务逻辑的元素，
    // 只提取出kv
    // Offset怎么来的？对着Kafka提交offset的API从哪里来？罪魁祸首就是第一个通过KafkaUtils创建的DStream，它自己提供的提交API、它内部包含的RDD提供了offset
    val dstream = kafka.map(record => {
      val topic = record.topic()
      val partition = record.partition
      val offset = record.offset
      val key = record.key
      val value = record.value
      (key, (value, topic, partition, offset))
    })
    dstream.print

    var ranges: Array[OffsetRange] = null
    // 完成了业务代码后提交offset
    // 正确的。放在dstream对象的接受函数里，那么未来在调度线程里，这个函数的每一个job都有机会调用一次，伴随着提交offset
    // 那个时间点才会用维护的offset？维护offset的语义是持久化，它在application或者Driver重启的时候会被用到
    kafka.foreachRDD(
      rdd => {
        println("foreachRDD...fun...")
        // Driver端可以拿到offset
        ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 闭包, 通过KafkaUtils得到的第一个DStream向上转型，提交offset
        kafka.asInstanceOf[CanCommitOffsets].commitAsync(ranges, new OffsetCommitCallback {
          override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
            if (offsets != null) {
              ranges.foreach(println)
              println("-----------------")
              val iter = offsets.keySet().iterator
              while (iter.hasNext) {
                val k = iter.next
                val v = offsets.get(k)
                println(s"${k.partition} ... ${v.offset}")
//                if (v.offset % 5 == 0) throw new RuntimeException("Exception")
              }
            }
          }
        }) // 放到外面去，其实不会起到作用
        // 如果维护到第三方MySQL：
        /*
          开启事务
          提交数据
          提交offset
          commit
         */
        // 提交两头都要堵。每次Subscribe的时候，要去读数据库，读出offset，以TopicPartition为key，offset为value，传进Subscribe方法
      }
    )

    // 上面的代码压在另一个线程里执行
    ssc.start()
    Thread.sleep(10000)

    ssc.awaitTermination()
  }
}

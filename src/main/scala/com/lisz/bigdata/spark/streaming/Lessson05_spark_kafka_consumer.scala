package com.lisz.bigdata.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
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
      (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "TRUE"),
      (ConsumerConfig.GROUP_ID_CONFIG, "test10")
      //(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")  // 不生效，要在conf里set
    )

    // 如何得到Kafka的DSream。
    val kafka: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent, // Spark的一个Consumer默认对应一个Kafka Partition，consumer运行在spark的executor里，每个consumer只能看到自己的数据
      ConsumerStrategies.Subscribe[String, String](List("topic1"), map)
    )
    //通过KafkaUtils得到的第一个DStream要先去转换一下，其实这个DStream就是consumer poll回来的records
    //将Kafka Record转换成业务逻辑元素：只是提取出key和value
    val dstream = kafka.map(record => {
      val topic = record.topic()
      val partition = record.partition()
      val offset = record.offset()
      val key = record.key()
      val value = record.value()

      (key, (value, topic, partition, offset))
    })
    dstream.print()



    ssc.start()
    ssc.awaitTermination()
  }

}

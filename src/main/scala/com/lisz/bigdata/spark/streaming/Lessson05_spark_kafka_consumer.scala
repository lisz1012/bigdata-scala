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
    val ssc = new StreamingContext(conf, Duration(1000))
    ssc.sparkContext.setLogLevel("ERROR")

    val map = Map[String, Object](
      (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop-02:9092,hadoop-03:9092,hadoop-04:9092"),
      (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer]),
      (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer]),
      (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
      (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "TRUE"),
      (ConsumerConfig.GROUP_ID_CONFIG, "test7")
    )

    // 如何得到Kafka的DSream
    val kafka: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
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

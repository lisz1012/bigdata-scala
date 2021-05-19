package com.lisz.bigdata.spark.streaming

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object Lesson04_kafka_producer {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop-02:9092,hadoop-03:9092,hadoop-04:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

    val producer = new KafkaProducer[String, String](props)
    while (true) {
      for (i <- 1 to 3;j <- 1 to 3) {
        val record = new ProducerRecord[String, String]("topic1", s"item$j", s"action$i")
        val records = producer.send(record)
        val metadata = records.get()
        val partition = metadata.partition()
        val offset = metadata.offset()
        println(s"item$j  sction$i  partition: $partition   offset: $offset")
      }
      println("------------")
      //Thread.sleep(1000)
    }
    producer.close()
  }

}

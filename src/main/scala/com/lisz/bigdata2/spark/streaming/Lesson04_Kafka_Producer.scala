package com.lisz.bigdata2.spark.streaming

import java.util.Properties
import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

object Lesson04_Kafka_Producer {


  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop-02:9092,hadoop-03:9092,hadoop-04:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

    val producer = new KafkaProducer[String, String](props)

    while (true) {
      for (i <- 1 to 3; j <- 1 to 3) {
        val record = new ProducerRecord[String, String]("topic1", s"item$j", s"action$i")
        val future: Future[RecordMetadata] = producer.send(record)
        val metadata = future.get()
        val offset = metadata.offset()
        val partition = metadata.partition()
        val topic = metadata.topic()
        val checksum = metadata.checksum()
        println(s"item$j action: $i partition: $partition offset: $offset topic: $topic checksum: $checksum")
      }

      Thread.sleep(1000)
    }

    producer.close
  }
}

package com.lisz.bigdata.spark.streaming

import java.time.Duration
import java.util
import java.util.Properties
import java.util.regex.Pattern

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

object Lesson04_kafka_consumer {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop-02:9092,hadoop-03:9092,hadoop-04:9092")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest") // consumer组从误导有的时候从什么位置开始取消息： earliest从组的CURRENT-OFFSET开始，CURRENT-OFFSET初始值为0，latest从LOG-END-OFFSET开始
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "FALSE") // 设置为FALSE的时候，Kafka不会维护CURRENT-OFFSET，这个值默认在Kafka这里查不到，除非程序把这个offset写回Kafka
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test4")
    //props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")

    val consumer = new KafkaConsumer[String, String](props)
//    val list = new util.ArrayList[String]()
//    list.add("topic1")
//    consumer.subscribe(list)
    consumer.subscribe(Pattern.compile("topic1"), new ConsumerRebalanceListener {
      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
        println(s"onPartitionsRevoked")
        val iter = partitions.iterator()
        while (iter.hasNext) {
          println(iter.next)
        }
      }

      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
        println(s"onPartitionsAssigned")  //这个Consumer负责那些分区
        val iter = partitions.iterator()
        while (iter.hasNext) {
          println(iter.next)
        }
        // 调用数据库取出offset
        consumer.seek(new TopicPartition("topic1", 1), 3704)
        consumer.seek(new TopicPartition("topic1", 2), 3704)
        Thread.sleep(5000)
      }
    })
    while (true) {
      val records = consumer.poll(0)
      if (!records.isEmpty) {
        println(s"-------------${records.count}-------------")
        val iter = records.iterator()
        while (iter.hasNext) {
          val record = iter.next()
          val topic = record.topic()
          val partition = record.partition()
          val offset = record.offset()
          val key = record.key()
          val value = record.value()
          println(s"key: $key  value: $value partition: $partition offset: $offset topic: $topic")
        }
      }
      Thread.sleep(500)
    }
  }

}

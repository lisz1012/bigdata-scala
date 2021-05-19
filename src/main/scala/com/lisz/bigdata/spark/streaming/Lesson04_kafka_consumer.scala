package com.lisz.bigdata.spark.streaming

import java.time.Duration
import java.util
import java.util.Properties
import java.util.regex.Pattern

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer, OffsetAndMetadata}
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
        Thread.sleep(5000)
      }
    })

    val offmap = new util.HashMap[TopicPartition, OffsetAndMetadata]()
    var record : ConsumerRecord[String, String] = null
    while (true) {
      val records = consumer.poll(0)
      if (!records.isEmpty) {
        println(s"-------------${records.count}-------------")
        val iter = records.iterator()
        while (iter.hasNext) {
          record = iter.next()
          val topic = record.topic()
          val partition = record.partition()
          val offset = record.offset()
          val key = record.key()
          val value = record.value()
          println(s"key: $key  value: $value partition: $partition offset: $offset topic: $topic")
        }
        // 手动维护offset有两类地方,防止丢失数据 1.手动维护到Kafka，先计算，再维护回offset 2. zk, MySQL...
        val partition = new TopicPartition("topic1", record.partition())
        val offset = new OffsetAndMetadata(record.offset())
        offmap.put(partition, offset)
        consumer.commitSync(offmap)
      }
      Thread.sleep(500)
    }
  }

}

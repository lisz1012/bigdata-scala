package com.lisz.bigdata2.spark.streaming

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

object Lesson04_Kafka_Consumer {

  /*
    KafkaConsumer
    1。 自动维护offset，poll数据之后先去写offset，然后去计算，可能会有数据丢失
    2。 手动维护offset：
        a。维护到Kafka自己的__consumer_offset topic中 且你还能通过 kafka-consumer-groups.sh查看
        b. 维护到其他的位置比如MySQL、zk、Redis 牵扯到 ConsumerRebalanceListener onPartitionsAssigned方法回调后，自己seek到要查询的位置
    3。 AUTO_OFFSET_RESET_CONFIG 自适应，必须参考__consumer_offsets
        earliest, latest  earliest一般在程序以某个组第一次启动的时候才会跟latest结果不一样
    4.  Consumer.seek覆盖性是最强的
   */
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put(BOOTSTRAP_SERVERS_CONFIG, "hadoop-02:9092,hadoop-03:9092,hadoop-04:9092")
    props.put(KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    props.put(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    props.put(AUTO_OFFSET_RESET_CONFIG, "latest") // earliest, latest  earliest一般在程序以某个组第一次启动的时候才会跟latest结果不一样
    props.put(ENABLE_AUTO_COMMIT_CONFIG, "FALSE")
    props.put(GROUP_ID_CONFIG, "g5");
//    props.put(MAX_POLL_RECORDS_CONFIG, 100)

    val consumer = new KafkaConsumer[String, String](props)
    val listener = new ConsumerRebalanceListener() {
      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
        println(s"onPartitionsRevoked")
        val iter = partitions.iterator()
        while (iter.hasNext) {
          println(iter.next)
        }
      }

      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
        println(s"onPartitionsAssigned")
        val iter = partitions.iterator
        while (iter.hasNext) {
          println(iter.next)
        }
        // 自己维护Kafka Offset的时候8712是从Redis或者MySQL取出来的. 客户端在自己的进程里会维护自己的消费位置在内存里，就是开启了个循环问broker要消息
        consumer.seek(new TopicPartition("topic1", 1), 9069)
        Thread.sleep(5000)
      }
    }

    consumer.subscribe(util.Arrays.asList("topic1"), listener)
    val map = new util.HashMap[TopicPartition, OffsetAndMetadata]()
    var record : ConsumerRecord[String, String] = null
    while (true) {
      val records = consumer.poll(10)
      var offset = 0L
      if (!records.isEmpty) {
        println(s"------------- ${records.count()} -------------")
        val iter = records.records("topic1").iterator()
        while (iter.hasNext) {
          record = iter.next
          println(s"topic: ${record.topic} partition: ${record.partition} " +
            s"offset: ${record.offset} key: ${record.key} value: ${record.value} ")
          offset = record.offset
        }
        // 可以维护offset到MySQL或者Redis
        val partition = new TopicPartition("topic1", record.partition())
        val offsetMeta = new OffsetAndMetadata(record.offset)

        map.put(partition, offsetMeta)
        consumer.commitSync(map)
        // 如果在运行时：手动提交offset到mysql， mysql --> listener， 通过 ConsumerRebalanceListener协商后获得的分区，去MySQL查询该分区上次消费到哪里了
      }
    }

    consumer.close
  }
}

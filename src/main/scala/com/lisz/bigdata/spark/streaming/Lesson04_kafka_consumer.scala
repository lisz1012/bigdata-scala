package com.lisz.bigdata.spark.streaming

import java.time.Duration
import java.util
import java.util.Properties
import java.util.regex.Pattern

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * kafka-consumer
 * 1，自动维护offset：ENABLE_AUTO_COMMIT_CONFIG  true   poll数据之后先去写offset，在去计算，会有丢失数据
 * 2，手动维护offset：ENABLE_AUTO_COMMIT_CONFIG  false
 *    a)维护到kafka自己的__consumer_offset_   这个topic中  且你还能通过  kafka-consumer-groups.sh  查看
 *    b)维护到其他的位置：mysql，zk
 *      *)这时候会比维护到Kafka多一步骤，牵扯到通过：ConsumerRebalanceListener  onPartitionsAssigned 方法回调后 自己seek到查询的位置
 *3，AUTO_OFFSET_RESET_CONFIG   自适应  必须参考  __consumer_offset_维护的
 *        //earliest    按   CURRENT-OFFSET   特殊状态：  group第一创建的时候，0
 *        // latest     按   LOG-END-OFFSET
 *4，seek覆盖性最强的，seek会覆盖上面所有的配置
 *
 * 在一个Consumer Group里，一个Partition只能对应一个Consumer，一个Consumer能消费多个partition。每个不同的组，有自己的消费偏移
 *
 *
 */

object Lesson04_kafka_consumer {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop-02:9092,hadoop-03:9092,hadoop-04:9092")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest") // consumer组从误导有的时候从什么位置开始取消息： earliest从组的CURRENT-OFFSET开始，CURRENT-OFFSET初始值为0，latest从LOG-END-OFFSET开始
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "FALSE") // 设置为FALSE的时候，Kafka不会维护CURRENT-OFFSET，这个值默认在Kafka这里查不到，除非程序把这个offset写回Kafka
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test5")
    //props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1") //一次最多拉取多少条消息

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
        //如果在运行时手动提交offset到MySQL，那么一旦程序重启，可以通过ConsumerRebalanceListener协商后获得的分区取MySQL查询该分区上次消费记录的位置
      }
      Thread.sleep(500)
    }
  }

}

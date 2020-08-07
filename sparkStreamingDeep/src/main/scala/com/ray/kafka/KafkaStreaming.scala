package com.ray.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable

object KafkaStreaming {


  def main(args: Array[String]): Unit = {
    //创建 SparkConf 对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaStreaming")
    //创建 StreamingContext 对象
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))
    //kafka 参数声明
    val brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    val topic = "first"
    val group = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"
    //定义 Kafka 参数
    val kafkaPara: Map[String, String] = Map[String, String](
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )
    //创建 KafkaCluster（维护 offset）
    val kafkaCluster = new KafkaCluster(kafkaPara)

    //获取 ZK 中保存的 offset
    val fromOffset: Map[TopicAndPartition, Long] = getOffsetFromZookeeper(kafkaCluster, group,
      Set(topic))
    //读取 kafka 数据创建 DStream
    val kafkaDStream: InputDStream[String] = KafkaUtils.createDirectStream[String, String,
      StringDecoder, StringDecoder, String](ssc,
      kafkaPara,
      fromOffset,
      (x: MessageAndMetadata[String, String]) => x.message())
    //数据处理
    kafkaDStream.print
    //提交 offset
    offsetToZookeeper(kafkaDStream, kafkaCluster, group)
    ssc.start()
    ssc.awaitTermination()
  }

  //从 ZK 获取 offset
  def getOffsetFromZookeeper(kafkaCluster: KafkaCluster, kafkaGroup: String, kafkaTopicSet:
  Set[String]): Map[TopicAndPartition, Long] = {
    // 创建 Map 存储 Topic 和分区对应的 offset
    val topicPartitionOffsetMap = new mutable.HashMap[TopicAndPartition, Long]()
    // 获取传入的 Topic 的所有分区
    // Either[Err, Set[TopicAndPartition]] : Left(Err) Right[Set[TopicAndPartition]]
    val topicAndPartitions: Either[Err, Set[TopicAndPartition]] =
    kafkaCluster.getPartitions(kafkaTopicSet)
    // 如果成功获取到 Topic 所有分区
    // topicAndPartitions: Set[TopicAndPartition]
    if (topicAndPartitions.isRight) {
      // 获取分区数据
      // partitions: Set[TopicAndPartition]
      val partitions: Set[TopicAndPartition] = topicAndPartitions.right.get
      // 获取指定分区的 offset
      // offsetInfo: Either[Err, Map[TopicAndPartition, Long]]
      // Left[Err] Right[Map[TopicAndPartition, Long]]
      val offsetInfo: Either[Err, Map[TopicAndPartition, Long]] =
      kafkaCluster.getConsumerOffsets(kafkaGroup, partitions)
      if (offsetInfo.isLeft) {
        // 如果没有 offset 信息则存储 0
        // partitions: Set[TopicAndPartition]
        for (top <- partitions)
          topicPartitionOffsetMap += (top -> 0L)
      } else {
        // 如果有 offset 信息则存储 offset
        // offsets: Map[TopicAndPartition, Long]
        val offsets: Map[TopicAndPartition, Long] = offsetInfo.right.get
        for ((top, offset) <- offsets)
          topicPartitionOffsetMap += (top -> offset)
      }
    }
    topicPartitionOffsetMap.toMap
  }

  //提交 offset
  def offsetToZookeeper(kafkaDstream: InputDStream[String], kafkaCluster: KafkaCluster,
                        kafka_group: String): Unit = {
    kafkaDstream.foreachRDD {
      rdd =>
        // 获取 DStream 中的 offset 信息
        // offsetsList: Array[OffsetRange]
        // OffsetRange: topic partition fromoffset untiloffset
        val offsetsList: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 遍历每一个 offset 信息，并更新 Zookeeper 中的元数据
        // OffsetRange: topic partition fromoffset untiloffset
        for (offsets <- offsetsList) {
          val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
          // ack: Either[Err, Map[TopicAndPartition, Short]]
          // Left[Err]
          // Right[Map[TopicAndPartition, Short]]
          val ack: Either[Err, Map[TopicAndPartition, Short]] =
          kafkaCluster.setConsumerOffsets(kafka_group, Map((topicAndPartition, offsets.untilOffset)))
          if (ack.isLeft) {
            println(s"Error updating the offset to Kafka cluster: ${ack.left.get}")
          } else {
            println(s"update the offset to Kafka cluster: ${offsets.untilOffset} successfully")
          }
        }
    }
  }
}
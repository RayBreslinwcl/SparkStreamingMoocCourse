package com.hadoop.kafka10

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/11/22.
  * kafka0.10版本之上的兼容
  */
object KafkaReceiver {
  def main(args: Array[String]): Unit = {


    if (args.length != 2) {
      System.err.println("Usage: KafkaDirect <brokers> <topics>")
      System.exit(1)
    }

    val Array(input_brokers,input_topics)=args


    val sparkconf = new SparkConf().setAppName("KafkaReceiver").setMaster("local[2]")
    val sc = new SparkContext(sparkconf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> input_brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

//    val topics = Array("log_flume")
    val topics=Array(input_topics)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(record => (record.key, record.value)).print()


    ssc.start()
    ssc.awaitTermination()

  }

}
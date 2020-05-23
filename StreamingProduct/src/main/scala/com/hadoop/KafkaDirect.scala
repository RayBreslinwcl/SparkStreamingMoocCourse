//package com.hadoop
//
//import kafka.serializer.StringDecoder
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//
///**
//  * Created by Administrator on 2018/9/21.
//  */
//object KafkaDirect {
//  def main(args: Array[String]): Unit = {
//    if(args.length!=2){
//      System.err.println("Usage: KafkaDirect <brokers> <topics>")
//      System.exit(1)
//    }
//
//    val Array(brokers,topics)=args
//    val sparkConf=new SparkConf().setAppName("KafkaDirect")
//      .setMaster("local[2]")
//
//    val ssc=new StreamingContext(sparkConf,Seconds(5))
//
//
//    val topicsSet=topics.split(",").toSet
//    val local_topicSet2="log_flume".split(",").toSet
//
//    val kafkaParams=Map[String,String]("metadata.broker.list"->"cdh1:9092")
//    //TODO: Spark streaming如何对接kafka
//    //参考源码createStream
//    val messages =KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
//      ssc,kafkaParams,local_topicSet2
//    )
//    //取第2个
//    messages.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}

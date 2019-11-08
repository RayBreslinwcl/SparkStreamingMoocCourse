package com.hadoop

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/11/22.
  */
object KafkaReceiver {
  def main(args: Array[String]): Unit = {
    val master="spark://hadoop:7077"

//    val sparkconf=new SparkConf().setAppName("KafkaReceiver").setMaster(master)
//      .set("spark.driver.host","hadoop")// "spark.driver.host"
    val sparkconf=new SparkConf().setAppName("KafkaReceiver").setMaster("local[2]")
    val sc=new SparkContext(sparkconf)
    val ssc=new StreamingContext(sc,Seconds(5))

    //kafka连接信息
    val zk="hadoop:2181/kafka09"
    val id="test"
    val topicset=Map[String,Int]("hello9"->1)
    import org.apache.spark.streaming.kafka._
    //def createStream(ssc : org.apache.spark.streaming.StreamingContext, zkQuorum : scala.Predef.String,
    // groupId : scala.Predef.String, topics : scala.Predef.Map[scala.Predef.String, scala.Int], storageLevel : org.apache.spark.storage.StorageLevel = { /* compiled code */ })
    val kafkaStream = KafkaUtils.createStream(ssc,zk,id,topicset)

    val DStream=kafkaStream.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

//      DStream.foreachRDD(RDD=>{
//            RDD.foreachPartition(partitions=>{
//              partitions.foreach(println)
//            })
//          })


    ssc.start()
    ssc.awaitTermination()

  }

}

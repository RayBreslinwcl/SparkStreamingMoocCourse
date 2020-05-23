//package com.project
//
//import com.Utils.DateUtils
//import com.domain.Clicklog
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
///**
//  * Created by Administrator on 2018/11/22.
//  */
//object StreamingApp {
//
//  def main(args: Array[String]): Unit = {
//    val sparkconf=new SparkConf().setAppName("KafkaReceiver").setMaster("local[2]")
//    val sc=new SparkContext(sparkconf)
//    val ssc=new StreamingContext(sc,Seconds(5))
//
//    //kafka连接信息
//    val zk="hadoop:2181/kafka_09_streaming"
//    val id="test"
//    val topicset=Map[String,Int]("hello_topic"->1)
//    import org.apache.spark.streaming.kafka._
//    //def createStream(ssc : org.apache.spark.streaming.StreamingContext, zkQuorum : scala.Predef.String,
//    // groupId : scala.Predef.String, topics : scala.Predef.Map[scala.Predef.String, scala.Int], storageLevel : org.apache.spark.storage.StorageLevel = { /* compiled code */ })
//    val kafkaStream = KafkaUtils.createStream(ssc,zk,id,topicset)
//
//    //步骤一：测试数据连接【pass】
////    val Dstream=kafkaStream.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()
//
//    //步骤二：数据清洗
//    val logs=kafkaStream.map(_._2) //.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()
//    val cleanData=logs.map(line=>{
//      val infos=line.split("\t")
//
//      val ip=infos(0)
//      val time=DateUtils.parseToMinute(infos(1))
//
//      val url=infos(2).split(" ")(1)
//      var courseId=0
//      if(url.startsWith("/class")){
//        val courseIdHTML=url.split("/")(2)
//        courseId=courseIdHTML.substring(0,courseIdHTML.indexOf(".")).toInt
//      }
//      Clicklog(ip,time,courseId,infos(3).toInt,infos(4))
//    }).filter(Clicklog=>Clicklog.couserId!=0) //过滤
//
//    cleanData.print()
//
//
//
//
//    ssc.start()
//    ssc.awaitTermination()
//
//  }
//}

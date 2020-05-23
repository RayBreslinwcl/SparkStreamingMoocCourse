package com.project

import com.Utils.DateUtils
import com.domain.Clicklog
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2018/11/22.
  */
object StreamingApp {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: KafkaDirect <brokers> <topics>")
      System.exit(1)
    }

    val Array(input_brokers,input_topics)=args

    val sparkconf=new SparkConf().setAppName("StreamingApp").setMaster("local[2]")
    val sc=new SparkContext(sparkconf)
    val ssc=new StreamingContext(sc,Seconds(5))


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> input_brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_Streaming",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    //    val topics = Array("log_flume")
    val topics=Array(input_topics)
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )


    //步骤一：测试数据连接【pass】
//    val Dstream=kafkaStream.map(_.value()).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    //步骤二：数据清洗
    val logs=kafkaStream.map(_.value()) //.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()
    val cleanData=logs.map(line=>{
      val infos=line.split("\t")

      val ip=infos(0)
      val time=DateUtils.parseToMinute(infos(1))

      val url=infos(2).split(" ")(1)
      var courseId=0
      if(url.startsWith("/class")){
        val courseIdHTML=url.split("/")(2)
        courseId=courseIdHTML.substring(0,courseIdHTML.indexOf(".")).toInt
      }
      Clicklog(ip,time,courseId,infos(3).toInt,infos(4))
    }).filter(Clicklog=>Clicklog.couserId!=0) //过滤

    cleanData.print()




    ssc.start()
    ssc.awaitTermination()

  }
}

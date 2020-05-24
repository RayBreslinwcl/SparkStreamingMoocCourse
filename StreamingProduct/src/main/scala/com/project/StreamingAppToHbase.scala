package com.project

import com.Utils.{DateUtils, MysqlUtils}
import com.dao.{WebClickCountDAO, WebSearchClickCountDao}
import com.domain.{Clicklog, WebClickCount, WebSearchClickCount}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2018/11/22.
  */
object StreamingAppToHbase {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: KafkaDirect <brokers> <topics>")
      System.exit(1)
    }

    val Array(input_brokers,input_topics)=args


    val sparkconf=new SparkConf().setAppName("KafkaReceiver").setMaster("local[2]")
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
    //val Dstream=kafkaStream.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    //步骤二：数据清洗【pass】
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

    //第三步：统计今天为止的网站访问量【pass】
    //HBase中rowkey操作20171111_9
    cleanData.map(x=>{
      (x.time.substring(0,8)+"_"+x.couserId,1)
    }).reduceByKey(_+_).foreachRDD(RDD=>{
      RDD.foreachPartition(partitions=>{

        //存储mysql(1)
//        val connection=MysqlUtils.createConnection()

        val list=new ListBuffer[WebClickCount]
        partitions.foreach(pair=>{
          list.append(WebClickCount(pair._1,pair._2))
          //存储mysql(2)【20181223添加】
//          val sql=" insert into imooc_course_clickcount(day_course, click_count) values('" + pair._1 + "'," + pair._2 + ") on duplicate key update click_count=click_count+"+pair._2
//          val sql=" insert into imooc_course_clickcount(day_course, course_id,click_count) values('" + pair._1 + "','"+ pair._1.substring(9,12) + "'," + pair._2 + ") on duplicate key update click_count=click_count+"+pair._2
//          connection.createStatement().execute(sql)
        })
        WebClickCountDAO.save(list)

      })
    })


    //第四步：统计从搜索殷勤过来的今天到现在为止实战课程的访问量
    //原始数据 x.referer=https://search.yahoo.com/search?p=Storm实战-》目标search.yahoo.com
    //存储到hbase数据格式：20191109_www.baidu.com_131  column=info:click_count, timestamp=1573273644333, value=\x00\x00\x00\x00\x00\x00\x00\x02
//    cleanData.map(x=>{
//      val referer=x.referer.replaceAll("//","/")
//      val splits=referer.split("/")
//      var host=""
//      if(splits.length>2){
//        host=splits(1)
//        }
//      (host,x.couserId,x.time)
//    }
//    ).filter(_._1!="").map(x=>{
//      (x._3.substring(0,8)+"_"+x._1+"_"+x._2,1)
//    }).reduceByKey(_+_).foreachRDD(RDD=>{
//      RDD.foreachPartition(partitions=>{
//        val list=new ListBuffer[WebSearchClickCount]
//        partitions.foreach(pair=>{
//          list.append(WebSearchClickCount(pair._1,pair._2))
//        })
//        WebSearchClickCountDao.save(list)
//      })
//    })


    ssc.start()
    ssc.awaitTermination()

  }
}

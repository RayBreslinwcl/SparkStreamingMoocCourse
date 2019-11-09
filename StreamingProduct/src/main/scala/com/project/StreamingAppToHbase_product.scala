package com.project

import com.Utils.DateUtils
import com.dao.{CourseClickCountDAO, CourseSearchClickCountDao}
import com.domain.{Clicklog, CourseClickCount, CourseSearchClickCount}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import com.Utils.MysqlUtils

/**
  * Created by Administrator on 2018/11/22
  */
object StreamingAppToHbase_product {

  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setAppName("KafkaReceiver").setMaster("local[3]")
    val sc=new SparkContext(sparkconf)
    val ssc=new StreamingContext(sc,Seconds(5))

    //生产环境
    //hadoop:2181/kafka09 test hello9 1
    val Array(zk,group,topic,numThreads)=args
    //kafka连接信息
//    val zk="hadoop:2181/kafka09"
//    val id="test"
//    val topicset=Map[String,Int]("hello9"->1)
    val topicMap=topic.split(",").map((_,numThreads.toInt)).toMap
    import org.apache.spark.streaming.kafka._
    //def createStream(ssc : org.apache.spark.streaming.StreamingContext, zkQuorum : scala.Predef.String,
    // groupId : scala.Predef.String, topics : scala.Predef.Map[scala.Predef.String, scala.Int], storageLevel : org.apache.spark.storage.StorageLevel = { /* compiled code */ })
    val kafkaStream = KafkaUtils.createStream(ssc,zk,group,topicMap)

    //步骤一：测试数据连接【pass】
    //val Dstream=kafkaStream.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    //步骤二：数据清洗【pass】
    val logs=kafkaStream.map(_._2) //.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()
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
//    cleanData.print()

    //第三步：统计今天为止的课程访问量【pass】【统计功能一20181124updtae】
//    HBase中rowkey操作20171111_9
    cleanData.map(x=>{
      (x.time.substring(0,8)+"_"+x.couserId,1)
    }).reduceByKey(_+_).foreachRDD(RDD=>{
      RDD.foreachPartition(partitions=>{

        //存储mysql(1)【20181223添加】
        val connection=MysqlUtils.createConnection()


        val list=new ListBuffer[CourseClickCount]
        partitions.foreach(pair=>{
          list.append(CourseClickCount(pair._1,pair._2))
          //存储mysql(2)【20181223添加】
          //          val sql=" insert into imooc_course_clickcount(day_course, click_count) values('" + pair._1 + "'," + pair._2 + ") on duplicate key update click_count=click_count+"+pair._2
          val sql=" insert into imooc_course_clickcount(day_course, course_id,click_count) values('" + pair._1 + "','"+ pair._1.substring(9,12) + "'," + pair._2 + ") on duplicate key update click_count=click_count+"+pair._2

        })
        CourseClickCountDAO.save(list)

      })
    })


    //第四步：统计从搜索引擎过来的今天到现在为止实战课程的访问量
    //原始数据x.referer= https://search.yahoo.com/search?p=Storm实战-》目标search.yahoo.com
    cleanData.map(x=>{
      val referer=x.referer.replaceAll("//","/")
      val splits=referer.split("/")
      var host=""
      if(splits.length>2){
        host=splits(1)
        }
      (host,x.couserId,x.time)
    }
    ).filter(_._1!="").map(x=>{
      (x._3.substring(0,8)+"_"+x._1+"_"+x._2,1)
    }).reduceByKey(_+_).foreachRDD(RDD=>{
      RDD.foreachPartition(partitions=>{
        val list=new ListBuffer[CourseSearchClickCount]
        partitions.foreach(pair=>{
          list.append(CourseSearchClickCount(pair._1,pair._2))
        })
        CourseSearchClickCountDao.save(list)
      })
    })


    ssc.start()
    ssc.awaitTermination()

  }
}

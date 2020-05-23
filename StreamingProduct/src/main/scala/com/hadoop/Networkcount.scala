package com.hadoop

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2018/11/25.
  */
object Networkcount {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
//      .set("spark.driver.host","119.3.92.224")
//      .set("spark.driver.port","4040")
    //conf.setMaster("spark://master01:7077");
    /***
      * 创建StreamingContext需要sparkConf和batch interval
      */
    val ssc=new StreamingContext(sparkConf,Seconds(5))

    val lines=ssc.socketTextStream("cdh3",6789)

    val result= lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    result.print()

//    result.foreachRDD(RDD=>{
//      RDD.foreachPartition(partitions=>{
//        partitions.foreach(println)
//      })
//    })


    ssc.start()
    ssc.awaitTermination()
  }


}

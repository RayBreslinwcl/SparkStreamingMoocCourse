package com.mysql

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2018/12/23.
  */
object streamingToMysql {
  /**
    * 获取mysql连接
    * @return
    */
  def createConnection()={
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://hadoop:3306/spark","root","123456")
  }

  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setAppName("StatefulWordcount").setMaster("local[2]")
    val ssc=new StreamingContext(sparkConf,Seconds(5))

    val lines=ssc.socketTextStream("hadoop",6789)
    val results=lines.flatMap( _.split(" "))
      .map((_,1)).reduceByKey(_+_)

    //TODO... 将结果写入到MYSQL
    //参考：http://spark.apache.org/docs/2.1.0/streaming-programming-guide.html
    results.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords =>{   //partitionOfRecords是整个分区的数据

        val connection = createConnection()
        partitionOfRecords.foreach(record =>{     //record这个record才是每一条数据
//        val sql=" insert into wordcount(word, wordcount) values('" + record._1 + "'," + record._2 + ")"
//          【更新为如果重复，就更新20181223update】
          val sql=" insert into wordcount(word, wordcount) values('" + record._1 + "'," + record._2 + ") on duplicate key update wordcount=wordcount+"+record._2
          connection.createStatement().execute(sql)
        })
        connection.close()

      }
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }
}

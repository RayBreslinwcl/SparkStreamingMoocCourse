package com.dao

import com.Utils.HBaseUtils
import com.domain.CourseClickCount
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2018/11/24.
  */
object CourseClickCountDAO {

  val tablename="imooc_course_clickcount"
  val cf="info"
  val qualifer="click_count"

  /**
    * 保存数据到hbase
    * @param list CourseClickCount集合
    */
  def save(list:ListBuffer[CourseClickCount])={

    val table=HBaseUtils.getInstance().getTable(tablename)
    for(ele<-list){
      table.incrementColumnValue(
        Bytes.toBytes(ele.day_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifer),
        ele.click_count
      )
    }
  }

  /**
    * 根据rowkey查询
    * @param day_course
    */
  def count(day_course:String): Long ={

    val table =HBaseUtils.getInstance().getTable(tablename)
    val get=new Get(Bytes.toBytes(day_course))
    val value=table.get(get).getValue(cf.getBytes(),qualifer.getBytes())

    if(value==null){
      0L
    }else{
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {

    val list=new ListBuffer[CourseClickCount]
    list.append(CourseClickCount("20171111_8",8))
    list.append(CourseClickCount("20171111_9",8))
    list.append(CourseClickCount("20171111_10",10))
    list.append(CourseClickCount("20171111_11",11))

    //测试一：写入
    save(list)

    //测试二：读取
    val c1=count("20171111_8")
    val c2=count("20171111_9")
    val c3=count("20171111_10")
    println(c1+":"+c2+":"+c3)
  }
}

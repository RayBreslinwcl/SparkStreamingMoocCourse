package com.dao

import com.Utils.HBaseUtils
import com.domain.{CourseClickCount, CourseSearchClickCount}
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2018/11/24.
  * 从搜索引擎过来的实战课程点击数的访问量
  */
object CourseSearchClickCountDao {
  val tablename="imooc_course_search_clickcount"
  val cf="info"
  val qualifer="click_count"

  /**
    * 保存数据到hbase
    * @param list CourseSearchClickCount集合
    */
  def save(list:ListBuffer[CourseSearchClickCount])={

    val table=HBaseUtils.getInstance().getTable(tablename)
    for(ele<-list){
      table.incrementColumnValue(
        Bytes.toBytes(ele.day_search_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifer),
        ele.click_count
      )
    }
  }

  /**
    * 根据rowkey查询
    * @param day_search_course
    */
  def count(day_search_course:String): Long ={

    val table =HBaseUtils.getInstance().getTable(tablename)
    val get=new Get(Bytes.toBytes(day_search_course))
    val value=table.get(get).getValue(cf.getBytes(),qualifer.getBytes())

    if(value==null){
      0L
    }else{
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {
    val list=new ListBuffer[CourseSearchClickCount]
    list.append(CourseSearchClickCount("20171111_www.baidu.com_8",8))
    list.append(CourseSearchClickCount("20171111_cn.bing.com_9",8))

    //测试一：保存
//    save(list)

    //测试二：读取
    //    println(count("20171111_8")+":"+count("20171111_9")+":"+count("20171111_10"))
    println(count("20171111_www.baidu.com_8")+":"+count("20171111_cn.bing.com_9"))

  }
}

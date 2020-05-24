package com.dao

import com.Utils.HBaseUtils
import com.domain.{WebClickCount, WebSearchClickCount}
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2018/11/24.
  * 从搜索引擎过来的web点击数的访问量
  */
object WebSearchClickCountDao {
  val tablename="web_search_clickcount"
  val cf="info"
  val qualifer="click_count"

  /**
    * 保存数据到hbase
    * @param list CourseSearchClickCount集合
    */
  def save(list:ListBuffer[WebSearchClickCount])={

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
    val list=new ListBuffer[WebSearchClickCount]
    list.append(WebSearchClickCount("20191111_www.baidu.com_8",8))
    list.append(WebSearchClickCount("20191111_cn.bing.com_9",8))

    //测试一：保存【20191109测试成功】
    save(list)

    //测试二：读取
    //    println(count("20171111_8")+":"+count("20171111_9")+":"+count("20171111_10"))
//    println(count("20171111_www.baidu.com_8")+":"+count("20171111_cn.bing.com_9"))

  }
}

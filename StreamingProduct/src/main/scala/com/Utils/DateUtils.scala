package com.Utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat


/**
  * Created by Administrator on 2018/11/22.
  */
object DateUtils {

  val YYYYMMDDHHMMSS_FORMAT=FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val TARGET_FORMAT=FastDateFormat.getInstance("yyyyMMddHHmmss")

  def getTime(time:String)={
    YYYYMMDDHHMMSS_FORMAT.parse(time).getTime
  }

  def parseToMinute(time:String) ={
    //异常处理：https://www.runoob.com/scala/scala-exception-handling.html
    try {
      TARGET_FORMAT.format(new Date(getTime(time)))
    } catch {
      case ex: Exception =>{
        println("parseToMinute failed，time is "+ time )
        "20200101"
      }
    }
//    TARGET_FORMAT.format(new Date(getTime(time)))
  }


  def main(args: Array[String]): Unit = {
    println(parseToMinute("手动发2018-11-22 14:14:444撒旦"))
  }

}

package com.domain

/**
  * Created by Administrator on 2018/11/24.
  */
/**
  *
  * @param day_course 对应的就是HBase中的rowkey
  * @param click_count 对应rowkey的访问总数
  */
case class CourseClickCount (day_course:String,click_count:Long)

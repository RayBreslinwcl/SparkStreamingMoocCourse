package com.Utils

import java.sql.DriverManager

/**
  * Created by Administrator on 2018/12/23.
  */
object MysqlUtils {
  /**
    * 获取mysql连接
    * @return
    */
  def createConnection()={
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://hadoop:3306/spark","root","123456")
  }
}

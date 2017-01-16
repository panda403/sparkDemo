package com.marstor.test

import java.sql.{Connection, DriverManager}


/**
  * Created by root on 11/1/16.
  */
object ScalaJdbcConnectSelect extends App{
  // 访问本地MySQL服务器，通过3306端口访问mysql数据库
  val url = "jdbc:mysql://localhost:3306/mysql"
  //驱动名称
  val driver = "com.mysql.jdbc.Driver"
  //用户名
  val username = "root"
  //密码
  val password = "123"
  //初始化数据连接
  var connection: Connection = _
  try {
    //注册Driver
    Class.forName(driver)
    //得到连接
    connection = DriverManager.getConnection(url, username, password)
    val statement = connection.createStatement
    //执行查询语句，并返回结果
    val rs = statement.executeQuery("SELECT host, user FROM user")
    //打印返回结果
    while (rs.next) {
      val host = rs.getString("host")
      val user = rs.getString("user")
      println("host = %s, user = %s".format(host, user))
    }
  } catch {
    case e: Exception => e.printStackTrace
  }
  //关闭连接，释放资源
  connection.close
}

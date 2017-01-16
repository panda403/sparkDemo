package com.marstor.usefultest

import java.sql.{DriverManager, PreparedStatement, Connection}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by root on 11/10/16.
  */
case class Blog(name: String, count: Int)
object RDD2Mysql {
  def myFun(iterator: Iterator[(String, Int)]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into blog(name, count) values (?, ?)"
    try {
      val MYSQL_USERNAME = "root"
      val MYSQL_PASSWORD = "12345678"
      val MYSQL_DRIVER = "com.mysql.jdbc.Driver"
      val MYSQL_URLS = "jdbc:mysql://192.168.2.190:3306/spark_dbtest?useUnicode=true&characterEncoding=utf8"
      val prop = new java.util.Properties()
      prop.setProperty("user",MYSQL_USERNAME)
      prop.setProperty("password",MYSQL_PASSWORD)
      prop.setProperty("driver",MYSQL_DRIVER)

      DriverManager.getDriver(MYSQL_DRIVER)
      conn = DriverManager.getConnection(MYSQL_URLS, prop)
      iterator.foreach(data => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, data._1)
        ps.setInt(2, data._2)
        ps.executeUpdate()
      }
      )
    } catch {
      case e: Exception => println("exception caught: " + e);
//      case e: Exception => println("mysql error")
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RDDToMysql").setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(List(("www", 10), ("iteblog", 20), ("com", 30)))
    data.foreachPartition(myFun)
  }
}

package com.marstor.spark

import java.sql.DriverManager

import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

/**
  * Created by root on 11/3/16.
  */
object SparkDBOperate {
  def main(args: Array[String]) {
    usingJdbcRDD()
  }
  def usingJdbcRDD(): Unit ={
    val sc = new SparkContext("local", "mysql")
    val rdd = new JdbcRDD(
      sc,
      () => {
        Class.forName("com.mysql.jdbc.Driver").newInstance()
        DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_dbtest", "root", "12345678")
      },
      "SELECT logTime FROM log_20120729_20120730 WHERE amount >= 500 ",
      1,10, 3,
      r => r.getString(1)).cache()

    print(rdd.saveAsTextFile("/panda/spark/tmp/jdbcrddresult.txt"))
//    print(rdd.filter(_.contains("success")).count())
  }
}

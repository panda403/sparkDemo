package com.marstor.demo

import java.io.File
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext

/**
  * panda的大数据田地 -- panda1234.com
  */
object SparkSQLHiveOnYarn {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("SparkSQLHiveOnYarn").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    import hiveContext.sql

    //在数据库panda中创建表 panda1234
    println("create table panda1234 .. ")
    sql("USE panda")
    sql("CREATE TABLE IF NOT EXISTS panda1234 (cate STRING, cate_id INT) STORED AS TEXTFILE")

    //从已存在的源表panda_cate_id插入数据到目标表panda1234
    println("insert data into table panda1234 .. ")
    sql("INSERT OVERWRITE TABLE panda1234 select cate,cate_id FROM panda_cate_id")

    //目标表panda1234的记录数
    println("Result of 'select count(1) from panda1234': ")
    val count = sql("SELECT COUNT(1) FROM panda1234").collect().head.getLong(0)
    println(s"panda1234 COUNT(1): $count")

    //源表panda_cate_id的记录数
    println("Result of 'select count(1) from panda_cate_id': ")
    val count2 = sql("SELECT COUNT(1) FROM panda_cate_id").collect().head.getLong(0)
    println(s"panda_cate_id COUNT(1): $count2")

    //目标表panda1234的limit 5记录
    println("Result of 'SELECT * from panda1234 limit 10': ")
    sql("SELECT * FROM panda1234 limit 5").collect().foreach(println)

    //sleep 10分钟，为了从WEB界面上看日志
    Thread.sleep(600000)
    sc.stop()

  }
}
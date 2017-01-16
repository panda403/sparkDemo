package com.marstor.demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 7/15/16.
  *
  */
object HiveTable {
  private val master = "master"
  private val port = "7077"
  private val appName = "HiveTables1"
//  val conf = new SparkConf().setAppName(appName).setMaster(s"spark://$master:$port")

  def main(argment: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(appName).setMaster("local[2]")
    val sc = new SparkContext(conf) // 建立spark操作上下
    // sc is an existing SparkContext.
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    sqlContext.sql("LOAD DATA LOCAL INPATH '/panda/spark/data/kv1.txt' INTO TABLE src")

    // Queries are expressed in HiveQL
    sqlContext.sql("FROM src SELECT key, value").collect().foreach(println)
  }
}

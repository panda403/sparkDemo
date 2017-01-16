//package com.marstor.test
//
//import java.text.SimpleDateFormat
//import java.util.Date
//
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
//import org.apache.spark.sql.{Row, SQLContext}
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
//
///**
//  * Created by root on 11/3/16.
//  */
//object MyLoggerApp {
//  def main(args: Array[String]): Unit = {
//
//    val path = "/panda/spark/logs/msp1.log"
//
//    println("<!--开始解析-->")
//    val reg = "^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}) ([\\s\\S]*)$"
//    val sc = new SparkContext(new SparkConf().setAppName("日志解析").setMaster("local[2]"))
//    val textRDD = sc.textFile(path)
//
//    /**
//      * 处理一条日志包括多行的情况
//      */
//    var key = ""
//    val formatRDD = textRDD.map { x =>
//      if (x.matches(reg)) {
//        key = x
//        Pair.apply(key, "")
//      } else {
//        Pair.apply(key, x)
//      }
//    }.reduceByKey((a, b) => {
//      a + "\n" + b
//    }).map(x => x._1 + x._2)
//
//
//    /**
//      * 将字符串转换为Logger
//      */
//    val loggerRDD: RDD[Logger] = formatRDD.map { x => {
//      val reg.r(time,  msg) = x //通过正则取值
//      val log = new Logger(formatDate(time), msg)
//      log
//    }
//    }.cache()
//
//    /**
//      * TODO 通过类的反射机制来定义数据库Scheme，但在scala语言中不知道为啥就是不成功，此处浪费了许久留着以后研究吧
//      */
//    /*val sqlc = new SQLContext(sc)
//    sqlc.createDataFrame(loggerRDD, classOf[Logger]).registerTempTable("logger")*/
//
//    /**
//      * 定义数据库Scheme
//      */
//    val schemaString = "time msg"
//    val schema =
//      StructType(
//        schemaString.split(" ").map(fieldName =>
//          if ("time".equals(fieldName))
//            StructField(fieldName, TimestampType, true)
//          else
//            StructField(fieldName, StringType, true)))
//    /**
//      * 将Logger转换为Row
//      */
//    val rowRDD = loggerRDD.map { log =>
//      Row(
//        formatDate(log.time),
//        log.msg)
//    }
//    /**
//      * 利用SQL进行查询过滤
//      */
//    //    val sqlc = bySQLContext(sc, rowRDD, schema);
//    val sqlc = byHiveContext(sc, rowRDD, schema);
//    val df = sqlc.sql("select * from logger where time between '2016-10-25 16:54:15' and '2016-10-25 16:56:35' order by time")
//    val errLogRDD = df.map { x =>
//      new Logger(
//        formatDate(x.getTimestamp(0)),
//        x.getString(1))
//    }
//    for (log <- errLogRDD.take(10)) {
//      println("time:" + formatDateToStr(log.time))
//      println("msg:" + log.msg)
//    }
//    println("<!--解析结束-->")
//  }
//
//  /**
//    * 创建临时表
//    */
//  def bySQLContext(sc: SparkContext, rowRDD: RDD[Row], schema: StructType): SQLContext = {
//    val sqlc = new SQLContext(sc)
//    sqlc.createDataFrame(rowRDD, schema).registerTempTable("logger")
//    sqlc
//  }
//
//  /**
//    * 创建永久表，需要提前搭建好Spark与Hive的集成环境
//    */
//  def byHiveContext(sc: SparkContext, rowRDD: RDD[Row], schema: StructType): SQLContext = {
//    val sqlc = new HiveContext(sc)
//    sqlc.sql("drop table if exists logger")
//    sqlc.sql("CREATE TABLE IF NOT EXISTS logger (time TIMESTAMP, msg STRING)")
//    sqlc.createDataFrame(rowRDD, schema).write.mode("overwrite").saveAsTable("logger")
//    sqlc
//  }
//
//  def formatDate(str: String): Date = {
//    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(str)
//  }
//
//  def formatDate(timestamp: java.sql.Timestamp): Date = {
//    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timestamp.toString())
//  }
//
//  def formatDate(date: Date): java.sql.Timestamp = {
//    new java.sql.Timestamp(date.getTime)
//  }
//
//  def formatDateToStr(date: Date): String = {
//    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date)
//  }
//}

package com.marstor.usefultest

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType

/**
  * Created by root on 11/3/16.
  */
object LoggerApp {
  def main(args: Array[String]): Unit = {
    println("<!--开始解析-->")
    val reg = "^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3}) (\\[.*\\]) (.*) (.*) - ([\\s\\S]*)$"
//    val path = "nova.log"
    val path = "hdfs://192.168.2.190:8020/test/spark/data/ziyuan/DSSPlatform-NEW.log.2016-10-09"
    val sc = new SparkContext(new SparkConf().setAppName("日志解析"))
    val textRDD = sc.textFile(path)

    /**
      * 处理一条日志包括多行的情况
      */
    var key = ""
    val formatRDD = textRDD.map { x =>
      if (x.matches(reg)) {
        key = x
        Pair.apply(key, "")
      } else {
        Pair.apply(key, x)
      }
    }.reduceByKey((a, b) => {
      a + "\n" + b
    }).map(x => x._1 + x._2)

    /**
      * 将字符串转换为Logger
      */
    val loggerRDD: RDD[Logger] = formatRDD.map { x => {
      val reg.r(time, thread, level, logger, msg) = x //通过正则取值
      val log = new Logger(formatDate(time), thread, level, logger, msg)
      log
    }
    }.cache()

    /**
      * TODO 通过类的反射机制来定义数据库Scheme，但在scala语言中不知道为啥就是不成功，此处浪费了许久留着以后研究吧
      */
    /*val sqlc = new SQLContext(sc)
    sqlc.createDataFrame(loggerRDD, classOf[Logger]).registerTempTable("logger")*/

    /**
      * 定义数据库Scheme
      */
    val schemaString = "time thread level logger msg"
    val schema =
      StructType(
        schemaString.split(" ").map(fieldName =>
          if ("time".equals(fieldName))
            StructField(fieldName, TimestampType, true)
          else
            StructField(fieldName, StringType, true)))
    /**
      * 将Logger转换为Row
      */
    val rowRDD = loggerRDD.map { log =>
      Row(
        formatDate(log.time),
        log.thread,
        log.level,
        log.logger,
        log.msg)
    }
    /**
      * 利用SQL进行查询过滤
      */
    //    val sqlc = bySQLContext(sc, rowRDD, schema);
    val sqlc = byHiveContext(sc, rowRDD, schema);
    val df = sqlc.sql("select * from logger where level='ERROR' and time between '2016-03-21 11:00:00' and '2016-03-21 12:00:00' order by time")
    val errLogRDD = df.map { x =>
      new Logger(
        formatDate(x.getTimestamp(0)),
        x.getString(1),
        x.getString(2),
        x.getString(3),
        x.getString(4))
    }
    for (log <- errLogRDD.take(10)) {
      println("time:" + formatDateToStr(log.time))
      println("thread:" + log.thread)
      println("level:" + log.level)
      println("logger:" + log.logger)
      println("msg:" + log.msg)
    }
    println("<!--解析结束-->")
  }

  /**
    * 创建临时表
    */
  def bySQLContext(sc: SparkContext, rowRDD: RDD[Row], schema: StructType): SQLContext = {
    val sqlc = new SQLContext(sc)
    sqlc.createDataFrame(rowRDD, schema).registerTempTable("logger")
    sqlc
  }

  /**
    * 创建永久表，需要提前搭建好Spark与Hive的集成环境
    */
  def byHiveContext(sc: SparkContext, rowRDD: RDD[Row], schema: StructType): SQLContext = {
    val sqlc = new HiveContext(sc)
    sqlc.sql("drop table if exists logger")
    sqlc.sql("CREATE TABLE IF NOT EXISTS logger (time TIMESTAMP, thread STRING, level STRING, logger STRING, msg STRING)")
    sqlc.createDataFrame(rowRDD, schema).write.mode("overwrite").saveAsTable("logger")
    sqlc
  }

  def formatDate(str: String): Date = {
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(str)
  }

  def formatDate(timestamp: java.sql.Timestamp): Date = {
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(timestamp.toString())
  }

  def formatDate(date: Date): java.sql.Timestamp = {
    new java.sql.Timestamp(date.getTime)
  }

  def formatDateToStr(date: Date): String = {
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(date)
  }
}

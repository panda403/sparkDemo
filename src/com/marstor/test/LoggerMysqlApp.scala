package com.marstor.test

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.marstor.sparksql.MyLoggerAppTest
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Created by root on 11/3/16.
  */
object LoggerMysqlApp {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("输出写入Mysql").setMaster("local[2]"))
    /**
      * 从hive中加载数据
      */
    val hivec = new HiveContext(sc)
    val df = hivec.sql("select * from logger")
    val loggerRDD = df.rdd.map { x =>
      new zyLogger(
        MyLoggerAppTest.formatDate(x.getTimestamp(0)),
        x.getString(1),
        x.getString(2),
        x.getString(3))
    }
    val resultRDD = loggerRDD.map { logger =>
//      Pair(formatDateToStr(logger.time), 1)
      Pair(logger.logLevel, 1)
    }.reduceByKey((a, b) => {
      a + b
    }).map(f =>
      Row(f._1, f._2)).sortBy(f => f.getInt(1), false, 2)
    for (r <- resultRDD.take(10)) {
      println(r.getString(0) + ":" + r.getInt(1))
    }
    /**
      * 定义数据库Scheme
      */
    val schemaString = "logLevel count"
    val schema =
      StructType(
        schemaString.split(" ").map(fieldName =>
          if ("logLevel".equals(fieldName))
            StructField(fieldName, StringType, true)
          else
            StructField(fieldName, IntegerType, true)))
    /**
      * TODO计算每分钟日志的个数
      */
    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", "root")
    connectionProperties.setProperty("password", "12345678")
    connectionProperties.setProperty("driver","com.mysql.jdbc.Driver")
    new SQLContext(sc).createDataFrame(resultRDD, schema).write.jdbc(
      "jdbc:mysql://192.168.2.190:3306/spark_dbtest",
      "logger_loglevel"+System.currentTimeMillis(),
      connectionProperties);
  }

  def formatDateToStr(date: Date): String = {
    new SimpleDateFormat("yyyy-MM-dd HH:mm").format(date)
  }
}

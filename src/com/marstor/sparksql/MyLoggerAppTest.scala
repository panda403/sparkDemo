package com.marstor.sparksql

import java.text.SimpleDateFormat
import java.util.Date

import com.marstor.test.zyLogger
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 11/3/16.
  */
object MyLoggerAppTest {
  def main(args: Array[String]): Unit = {

    //    val path = "/panda/spark/logs/msp1.log"
//    val path = "/panda/spark/logs/abc.txt"
//    val path = "/panda/spark/data/ziyuan/DSSPlatform-NEW.log.2016-10-*"
//    val path = "/panda/spark/logs/01"
    val path = "hdfs://192.168.2.190:8020/test/spark/data/ziyuan1"
    println("<!--开始解析-->")
    val reg = "^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}) (\\[.*\\)])(-\\[\\w{4,6}\\]) ([\\s\\S]*)$"
    val sc = new SparkContext(new SparkConf().setAppName("日志解析").setMaster("local[2]"))
    //需要开启SingleSession，否则看不见TempTable
//    sc.set("spark.sql.hive.thriftServer.singleSession", "true")

    System.getProperty("file.encoding")
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//    sc.setLogLevel("WARN")

    val textRDD = sc.textFile(path)
//    sc.textFile("/data/*.txt")

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

//        formatRDD.collect().foreach(println(_))
    //    formatRDD.saveAsTextFile("/panda/spark/tmp/rdd/"+System.currentTimeMillis())
    //  }

    /**
      * 将字符串转换为Logger
      */
    val loggerRDD: RDD[zyLogger] = formatRDD.map { x => {
      val reg.r(time,packageName,logLevel, msg) = x //通过正则取值
      val log = new zyLogger(formatDate(time),formatPackageName(packageName),formatLogLevel(logLevel), msg)
      log
    }
    }.cache()
//    println(loggerRDD.take(10).foreach(println(_)))
    /**
      * TODO 通过类的反射机制来定义数据库Scheme
      */
    //    import sqlContext.implicits._
//    val sqlc = new SQLContext(sc)
//    val loggerDF = sqlc.createDataFrame(loggerRDD, classOf[Logger])
//    loggerDF.registerTempTable("logger")
//    loggerDF.show(10)
//    loggerDF.select("packageName","logLevel").show(10)



//    /**
//      * 定义数据库Scheme
//      */
    val schemaString = "time packageName logLevel msg"
    val schema =
      StructType(
        schemaString.split(" ").map(fieldName =>
          if ("time".equals(fieldName))
            StructField(fieldName, TimestampType, true)
          else
            StructField(fieldName, StringType, true)))
//    /**
//      * 将Logger转换为Row
//      */
    val rowRDD = loggerRDD.map { log =>
      Row(
        formatDate(log.time),
        log.packageName,
        log.logLevel,
        log.msg)
    }
//    /**
//      * 利用SQL进行查询过滤
//      */
//
//    //    val sqlc = bySQLContext(sc, rowRDD, schema);
    val sqlc = byHiveContext(sc, rowRDD, schema);
    val df = sqlc.sql("select logLevel,count(logLevel) as levelCount from logger group by logLevel ")
    df.select("logLevel","levelCount").collect().foreach(println(_))
//    val df = sqlc.sql("select * from logger where msg like '%shape%' ")
//    df.select("logLevel","msg").collect().foreach(println(_))


//    val df = sqlc.sql("select * from logger where logLevel like '%ERROR%' order by time ")

//    println("logLevel count : "+df.count())
//    df.foreach(println(_))
//    val errorLogRDD = df.map { x =>
//      new Logger(
//        formatDate(x.getTimestamp(0)),
//        x.getString(1),
//        x.getString(2),
//        x.getString(3))
//    }
//    for (log <- errorLogRDD.take(10)) {
//      println("time:" + formatDateToStr(log.time))
//      println("packageName:"+log.packageName)
//      println("logLevel:"+log.logLevel)
//      println("msg:" + log.msg)
//      println("-------------------------------------------------------------")
//    }

    val MYSQL_USERNAME = "root"
    val MYSQL_PASSWORD = "12345678"
    val MYSQL_DRIVER = "com.mysql.jdbc.Driver"
    val MYSQL_URLS = ""
    val TABLE_NAME = "ziyuanlog"
    val sqlcommand = "select * from logger order by time"
//    val sqlcommand = "select * from logger where msg like '%shape%' order by time"
    val prop = new java.util.Properties()
    prop.setProperty("user",MYSQL_USERNAME)
    prop.setProperty("password",MYSQL_PASSWORD)
    prop.setProperty("driver",MYSQL_DRIVER)

    sqlc.sql(sqlcommand).write.mode(SaveMode.Append).jdbc(MYSQL_URLS,TABLE_NAME,prop)
//    sqlc.sql(sqlcommand).write.jdbc(MYSQL_URLS,TABLE_NAME,prop)

    sc.stop()


    println("mysql success ")

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
    sqlc.sql("CREATE TABLE IF NOT EXISTS logger (time TIMESTAMP, packageName STRING,logLevel STRING,msg STRING)")
    sqlc.createDataFrame(rowRDD, schema).write.mode("overwrite").saveAsTable("logger")
    sqlc
  }

  def formatDate(str: String): Date = {
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(str)
  }

  def formatDate(timestamp: java.sql.Timestamp): Date = {
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timestamp.toString())
  }

  def formatDate(date: Date): java.sql.Timestamp = {
    new java.sql.Timestamp(date.getTime)
  }

  def formatDateToStr(date: Date): String = {
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date)
  }


  def formatPackageName(packageName : String): String ={
    packageName.substring(1,packageName.length-1)
  }
  def formatLogLevel(logLevel : String): String ={
    logLevel.substring(2,logLevel.length-1)
  }

}

package com.marstor.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 7/23/16.
  */
case class Logs(logTime: String,userName: String, ipAddr : String,logDesc: String)
object SparkSQLDemo {

  def main(args: Array[String]): Unit = {
    println("start SparkSQLDemo....")

    val hdfsFilePath = "hdfs://192.168.2.190:8020/test/spark/data/radomdata/testfile.log"

//    if (args.length == 0) {
//      System.err.println("Usage: SparkSQLDemo <hdfsURL> <resultURL>")
//      System.exit(1)
//    }
//    setMaster("yarn-cluster")
    val conf = new SparkConf().setAppName("SparkSQLDemo").setMaster("local")
    val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")
//      Logger.getLogger("org").setLevel(Level.ERROR)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    val people = sc.textFile(hdfsFilePath).map(_.split("\t"))
      .map(p => Log(p(0), p(1),p(2),p(3))).toDF()

    people.registerTempTable("tmpLogs")

    //    people.coalesce(1,false).foreach(println)
    people.select("logTime","userName").show(10)
//    if(args.length > 1){
//      people.write.format("parquet").save(args(1))
//    }else{
//      println("no parquet....")
//    }

    val MYSQL_USERNAME = "root"
    val MYSQL_PASSWORD = "12345678"
    val MYSQL_DRIVER = "com.mysql.jdbc.Driver"
    val MYSQL_URLS = "jdbc:mysql://192.168.2.190:3306/spark_dbtest?useUnicode=true&characterEncoding=utf8"
    val TABLE_NAME = "log_20120729_20120730"
    val prop = new java.util.Properties()
    prop.setProperty("user",MYSQL_USERNAME)
    prop.setProperty("password",MYSQL_PASSWORD)
    prop.setProperty("driver",MYSQL_DRIVER)

    sqlContext.sql("select substr(logTime,1,13) as logTime ,count(*) as amount from tmpLogs where logTime <= '2012-07-30 00:00:00' and logTime >= '2012-07-29 00:00:00' group by " +
      "substr(logTime,1,13) order by amount desc").write.jdbc(MYSQL_URLS,TABLE_NAME,prop)

    println("mysql success")
  }
}

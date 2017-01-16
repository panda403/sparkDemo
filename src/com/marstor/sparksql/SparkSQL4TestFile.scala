package com.marstor.sparksql

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by root on 7/22/16.
  */
//case class Log(logTime: Timestamp,userName: String, ipAddr : String,logDesc: String)
case class Log(logTime: String,userName: String, ipAddr : String,logDesc: String)
object SparkSQL4TestFile {
  def main(argment: Array[String]): Unit = {
//    readDataFromHDFS()
//    writeLog2MySql(df)
    readDataFromMySql()
//    readDataFromParquet()
  }

  /**
    * read logs from existing mysql database
    */
  def readDataFromMySql(): Unit ={

    val MYSQL_USERNAME = "root"
    val MYSQL_PASSWORD = "12345678"
    val MYSQL_DRIVER = "com.mysql.jdbc.Driver"
    val MYSQL_URLS = "jdbc:mysql://192.168.2.190:3306/spark_dbtest"
    val TABLE_NAME = "province_city_country"
    val prop = new java.util.Properties()
    prop.setProperty("user",MYSQL_USERNAME)
    prop.setProperty("password",MYSQL_PASSWORD)
    prop.setProperty("driver",MYSQL_DRIVER)

    val conf = new SparkConf().setAppName("readDataFromMySql").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val people = sqlContext.read.jdbc(MYSQL_URLS,TABLE_NAME,prop)
//    val people = sqlContext.read.jdbc(MYSQL_URLS,TABLE_NAME,Array("userName='lisi'"),prop)
    people.printSchema()
    people.show(2)
//    people.filter(people.userName="lisi").show(2)
    /**
      * save dataframe to parquet file
      */
    people.write.save("home/panda/2000W/1229/province_city_country.csv")
//    people.write.parquet("hdfs://192.168.0.211:8020/test/sparksqltest/readDataFromMySql_output")
//    people.write.save("hdfs://192.168.0.211:8020/test/sparksqltest/readDataFromMySql_output")
    println("success")

  }

  /**
    * create dataframe using RDD from hdfs log files,save the file to parquet file or json file
    */
  def readDataFromHDFS(): DataFrame ={
    val hdfsFilePath = "hdfs://192.168.0.211:8020/test/sparksqltest/data/testfile.log"
    val conf = new SparkConf().setAppName("SparkSQL4TestFile")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    val people = sc.textFile(hdfsFilePath).map(_.split("\t")).map(p => Log(p(0), p(1),p(2),p(3))).toDF()

    people.registerTempTable("logs2")

//    people.coalesce(1,false).foreach(println)
    people.select("logTime","userName").show(2)

    /**
      * save as parquet
      */
//    people.write.format("parquet").save("hdfs://192.168.0.211:8020/test/sparksqltest/parquet/testfile")

    val MYSQL_USERNAME = "root"
    val MYSQL_PASSWORD = "12345678"
    val MYSQL_URLS = "jdbc:mysql://192.168.0.211:3306/spark_testdb"
    val TABLE_NAME = "logs_20120818_20120819"
    val prop = new java.util.Properties()
    prop.setProperty("user",MYSQL_USERNAME)
    prop.setProperty("password",MYSQL_PASSWORD)

    sqlContext.sql("select substr(logTime,1,13) ,count(*) as amount from logs2 where logTime <= '2012-08-19 00:00:00' and logTime >= '2012-08-18 00:00:00' group by " +
          "substr(logTime,1,13) order by amount").write.jdbc(MYSQL_URLS,TABLE_NAME,prop)

    println("mysql success")


//    people.select("logTime","userName").write.format("parquet").save("hdfs://192.168.0.211:8020/test/sparksqltest/parquet/testfile")
    /**
      * write the DataFrame to json file
      */
//    people.write.json("hdfs://192.168.0.211:8020/test/sparksqltest/testfile_output_json")

    /**
      *spark.sql.caseSensitive is true default,can set it to false
      */
//    sqlContext.setConf("spark.sql.caseSensitive","false")
//    people.select("ipAddr").show(2)

    /*
    * cant use
    * */
//    people.select("userName").write.text("hdfs://192.168.0.211:8020/test/sparksqltest/testfile_output_txt")
//    people.write.format(source).mode(mode).save(path).
//    people.

    //    sqlContext.sql("select substr(logTime,1,13) as logtime,count(*) as logamount from logs where logTime <= '2012-08-19 00:00:00' and logTime >= '2012-08-18 00:00:00' group by " +
    //      "substr(logTime,1,13) order by substr(logTime,1,13)").collect().foreach(println)

//    sqlContext.sql("select substr(logTime,1,13) ,count(*) as amount from logs2 group by " +
//      "substr(logTime,1,13) order by amount desc limit 10 ").collect().foreach(println)

//    sqlContext.sql("select substr(logTime,1,13) ,count(*) as amount from logs2 group by " +
//      "substr(logTime,1,13) order by amount desc limit 10 ").collect().foreach(println)

    return people;
    //    sqlContext.table("logs").groupBy("substr(logTime,1,13)").agg("substr(logTime,1,13)","count(*)").collect()

  }

  /**
    * read data from parquet file
    */
  def readDataFromParquet(): Unit ={

    val hdfsFilePath = "hdfs://192.168.0.211:8020/test/sparksqltest/parquet/testfile"
    val conf = new SparkConf().setAppName("readDataFromParquet").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._
    val parquetLogs = sqlContext.read.parquet(hdfsFilePath)
    parquetLogs.registerTempTable("parquestLog")
//    parquetLogs.show(2)

    sqlContext.sql("select substr(logTime,1,13) ,count(*) as amount from parquestLog group by " +
            "substr(logTime,1,13) order by amount desc limit 10 ").collect().foreach(println)

  }

  /*
  * write DataFram to mysql
  * */
  def writeLog2MySql(people : DataFrame): Unit ={
    println("start writeLog2MySql......")
    val MYSQL_USERNAME = "root"
    val MYSQL_PASSWORD = "12345678"
    val MYSQL_URLS = "jdbc:mysql://192.168.0.211:3306/spark_testdb"
    val TABLE_NAME = "logs2"
    val prop = new java.util.Properties()
    prop.setProperty("user",MYSQL_USERNAME)
    prop.setProperty("password",MYSQL_PASSWORD)
//    Class.forName("com.mysql.jdbc.Driver").newInstance()
//        write people to mysql database
    people.write.jdbc(MYSQL_URLS,TABLE_NAME,prop)
  }

}

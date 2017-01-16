package com.marstor.sparksql

/**
  * Created by root on 12/15/16.
  */

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.util.matching.Regex;

case class user( imei:String,logtime:String, region:String)
object SparkImmi {

  def main(args: Array[String]) {

    val filePath = "/home/panda/immi.log"

    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("SparkImmi")
    val sc  = new SparkContext(sparkConf)
    //val ssc = new StreamingContext(sc, Seconds(10))
    val sqlContext = new SQLContext(sc)
   
    val data = sc.textFile(filePath)

    //剔除type等于3的数据
    val notContainsType3 = data.filter(!_.contains("type:3")).filter(!_.contains("imei:")).filter(!_.contains("000000000000000")).filter(!_.contains("Unknown"))

    //过滤logid或imei不存在的数据
    val cleanData = notContainsType3.filter(_.contains("logid")).filter(_.contains("imei")).filter(_.contains("areacode"))

    val cleanMap = cleanData.map {
      line =>
          val data = formatLine(line).split(",")
        user(data(0),data(1),data(2))
    }

    import sqlContext.implicits._
    val  dfuser= cleanMap.toDF()
    dfuser.registerTempTable("user")

   // val testuser=sqlContext.sql("select region ,count(distinct imei) from user group by region order by count(distinct imei) desc ")

    //testuser.collect().foreach(println)

    //mysql中的配置表
    val jdbcDF = sqlContext.read.format("jdbc").options(
      Map("url"->"jdbc:mysql://localhost:3306/db_ldjs",
        "dbtable"->"(select imei,region,city,company,name from tb_user_imei) as some_alias",
        "driver"->"com.mysql.jdbc.Driver",
        "user"-> "root",
        //"partitionColumn"->"day_id",
        "lowerBound"->"0",
        "upperBound"-> "1000",
        //"numPartitions"->"2",
        "fetchSize"->"100",
        "password"->"123456")).load()
    jdbcDF.registerTempTable("userinfo")
//    dfuser.printSchema()
//    jdbcDF.printSchema()
//    dfuser.show()
//    jdbcDF.show()
    val testuser=sqlContext.sql("select A.imei,A.logtime,A.region,B.name,B.city,B.company from userinfo as B left join user as A on A.imei=B.imei ")
    testuser.collect().foreach(println)
//


  }
  
   /**
    * 从每行日志解析出imei和logid
    *
    **/
  def formatLine(line: String): String = {
      val logIdRegex = """"logid":"([0-9]+)",""".r
    val imeiRegex = """//"imei//"://"([A-Za-z0-9]+)//"""".r
    val regionRegex = """"areacode":"([^A-Za-z0-9_]+)",""".r    //""""areacode":"[//u4e00-//u9fa5]*"""".r
    val logId = getDataByPattern(logIdRegex, line)
    val imei = getDataByPattern(imeiRegex, line)
    val region = getDataByPattern(regionRegex, line)
    //时间取到秒
    imei + "," + logId.substring(0, 14)+ "," + region
  }

  /**
    * 根据正则表达式,查找相应值
    *
    **/
  def getDataByPattern(p: Regex, line: String): String = {
    val result = (p.findFirstMatchIn(line)).map(item => {
      val s = item group 1
      s
    })
    result.getOrElse("NULL")
  }

  /**
    * 根据时间字符串获取时间,单位(秒)
    *
    **/
  def getTimeByString(timeString: String): Long = {
    val sf: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    sf.parse(timeString).getTime / 1000
  }
}

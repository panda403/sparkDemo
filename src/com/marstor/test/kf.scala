package com.marstor.test

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 1/11/17.
  */
case class KFC(name: String, gender: String, ctfId: String, birthday: String,
                      address: String, constellation: String,kftime : String)

object kf {
  def main(args: Array[String]) {

    val startTime = System.currentTimeMillis();

    val conf = new SparkConf().setAppName("AnalysisKF").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val kfFilePath = "/home/panda/2000W/1229/qualified.csv"

    import sqlContext.implicits._

    val customerRdd = sc.textFile(kfFilePath, 200).map(_.split(","))
      .filter(line => line.length > 7)
      .filter(!_.contains("Name"))
        .filter(_.size > 31)
      .map(
        p => KFC(p(0), formatGender(p(4), p(5)), p(4), getBirthDay(p(4)), p(7), formatCln(p(4)),getKFHour(p(31)) ) )
    val customerDF = customerRdd.toDF()
    customerDF.registerTempTable("kfc")

//    val kfc_sql = "SELECT kftime,count(kftime) ccount FROM kfc group by kftime"
val kfc_sql = "SELECT * FROM kfc "
    val table_name = "kfc_all_date"


    val MYSQL_USERNAME = "root"
    val MYSQL_PASSWORD = "12345678"
    val MYSQL_DRIVER = "com.mysql.jdbc.Driver"
    val MYSQL_URLS = "jdbc:mysql://192.168.2.190:3306/spark_dbtest?useUnicode=true&characterEncoding=utf8"
    val TABLE_NAME = table_name
    val prop = new java.util.Properties()
    prop.setProperty("user",MYSQL_USERNAME)
    prop.setProperty("password",MYSQL_PASSWORD)
    prop.setProperty("driver",MYSQL_DRIVER)

    sqlContext.sql(kfc_sql).write.mode(SaveMode.Append).jdbc(MYSQL_URLS,TABLE_NAME,prop)

    println("run time is :"+(System.currentTimeMillis()-startTime)/1000 +" s")

    println("mysql success")

//    val kfc_sql = "SELECT kftime,count(kftime) FROM kfc group by kftime"
//    val result = sqlContext.sql(kfc_sql)
//    result.collect().foreach(println)
  }

  def getKFHour(kftime : String): String ={

    try {
      if (kftime.length < 10) {
        "1990-01-01 00:00:00"
      } else {
        kftime
//        kftime.split(" ")(1).split(":")(0)
//        kftime.split(" ")(0)
      }
    } catch {
      case ex:Exception => "error time:"+kftime
    }
  }
  def formatGender(ctfId: String, gender: String): String = {
    var rt = "未知"
    if (ctfId.length == 18) {
      try {
        if ((ctfId.substring(16, 17).toInt) % 2 == 0) {
          rt = "F"
        } else {
          rt = "M"
        }
      } catch {
        case _ => println("error ctfid is : " + ctfId)
      }
    } else {
    }
    rt
  }

  def getBirthDay(ctfId : String) : String = {
    ctfId.substring(6,12)
  }

  def toInt(s: String): Int = {
    try {
      s.toInt
    } catch {
      case e: Exception => 9999
    }
  }

  def formatCln(ctfId: String): String = {
    val birthday = ctfId.substring(6,14)
    var rt = "未知"
    if (birthday.length == 8) {
      val md = toInt(birthday.substring(4))
      if (md >= 120 & md <= 219)
        rt = "水瓶座"
      else if (md >= 220 & md <= 320)
        rt = "双鱼座"
      else if (md >= 321 & md <= 420)
        rt = "白羊座"
      else if (md >= 421 & md <= 521)
        rt = "金牛座"
      else if (md >= 522 & md <= 621)
        rt = "双子座"
      else if (md >= 622 & md <= 722)
        rt = "巨蟹座"
      else if (md >= 723 & md <= 823)
        rt = "狮子座"
      else if (md >= 824 & md <= 923)
        rt = "处女座"
      else if (md >= 924 & md <= 1023)
        rt = "天秤座"
      else if (md >= 1024 & md <= 1122)
        rt = "天蝎座"
      else if (md >= 1123 & md <= 1222)
        rt = "射手座"
      else if ((md >= 1223 & md <= 1231) | (md >= 101 & md <= 119))
        rt = "摩蝎座"
      else
        rt = "未知"
    }

    rt
  }
}

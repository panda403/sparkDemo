package com.marstor.sparksql

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.marstor.base.LogLevel
import org.apache.spark.sql.SaveMode
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

/**
  * Created by root on 11/25/16.
  */
//case class KFCustomer(name: String, gender: String, ctfId: String, birthday: String, address: String,constellation:String,mobile:String,nation:String,arriveTime:Timestamp)

//case class KFCustomer(name: String, gender: String, ctfId: String, birthday: String,
//                      address: String,constellation:String,mobile:String,nation:String,arriveTime: Timestamp)
case class KFCustomer(name: String, gender: String, ctfId: String, birthday: String,
                      address: String, constellation: String)

object AnalysisKF {

  val filePath = "/home/panda/2000W/1205/*.csv"
  val conf = new SparkConf().setAppName("AnalysisKF").setMaster("local[3]")
  val sc = new SparkContext(conf)
  val useRec = sc.accumulator(0, "useful Records")
  val uselessRec = sc.accumulator(0, "useless Records")

  def main(args: Array[String]) {

    LogLevel.setLogLevelWarn()
//    sc.setLogLevel("ERROR")
    //      Logger.getLogger("org").setLevel(Level.ERROR)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._


//        val customer = sc.textFile(filePath).map(_.split(",")).filter(line => line.length > 7).filter(!_.contains("Name")).map(
//          p => KFCustomer(p(0), p(5), p(4), p(6), p(7),formatCln(p(6)),p(16),p(20),formatStr2Timpstamp(p(31)))).toDF()

        val customer = sc.textFile(filePath).map(_.split(",")).filter(line => line.length > 7).filter(!_.contains("Name")).map(
          p => KFCustomer(p(0), formatGender(p(4),p(5)), p(4), p(6), p(7),formatCln(p(6)))).toDF()


//    val customer = sc.textFile(filePath).map(_.split(","))
//      .filter(line => line.length > 7)
//      .filter(!_.contains("Name"))
////      .filter(_(4).length == 18)
//      .map(
//      p => KFCustomer(p(0), formatGender(p(4), p(5)), p(4), p(6), p(7), formatCln(p(6)))).toDF()

    customer.printSchema()

    customer.registerTempTable("customer")


    //    var result = sqlContext.sql("select count(*),case length(ctfId) when 18 then 'use' else 'useless' end  dtype from customer group by dtype")
    //            result.collect().foreach(println)

    customer.show(10)
        println(customer.count())

//    var result = sqlContext.sql("SELECT gender, count(gender) FROM customer group by gender")
//    result.collect().foreach(println)

//    println("************************************************")
//    println(useRec.name.get + " " + useRec.value)
//
//    println(uselessRec.name.get + " " + uselessRec.value)
//    println("************************************************")
    //    var result = sqlContext.sql("SELECT distinct(gender) from customer ")
    //        result.collect().foreach(println)

    //    sqlContext.udf.register("constellation", (x: String) => formatCln(x))
    //    var result = sqlContext.sql("SELECT constellation(birthday) , count(constellation(birthday))  ccount FROM customer group by constellation(birthday) order by ccount desc")
    //    result.collect().foreach(println)

    //    var result2 = sqlContext.sql("SELECT * from customer where name like '%王%' ")
    //    result2.collect().foreach(println)

    /**
      * create table sql :create table kfc(name varchar(100),gender varchar(5),ctfId varchar(50),birthday varchar(20),address varchar(250),constellation varchar(20));
      * create table kfc(id int primary key not null auto_increment, name varchar(100),gender varchar(5),ctfId varchar(50),birthday varchar(20),address varchar(250),constellation varchar(20),mobile varchar(50),nation varchar(30),arriveTime timestamp);
      */
//        val MYSQL_USERNAME = "root"
//        val MYSQL_PASSWORD = "12345678"
//        val MYSQL_DRIVER = "com.mysql.jdbc.Driver"
//        val MYSQL_URLS = "jdbc:mysql://192.168.2.190:3306/spark_dbtest?useUnicode=true&characterEncoding=utf8"
//        val TABLE_NAME = "kfc"
//        val prop = new java.util.Properties()
//        prop.setProperty("user",MYSQL_USERNAME)
//        prop.setProperty("password",MYSQL_PASSWORD)
//        prop.setProperty("driver",MYSQL_DRIVER)
//
//        sqlContext.sql("select * from customer").write.mode(SaveMode.Append).jdbc(MYSQL_URLS,TABLE_NAME,prop)
//
//        println("mysql success")
    


  }

    def formatStr2Timpstamp(str: String): java.sql.Timestamp = {
  //    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(str)
      if(str != "Version"){
        new Timestamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(str).getTime)
      }else{
        new Timestamp(new java.util.Date().getTime())
      }
  //    java.util.Date utilDate = new java.util.Date();
  //    java.sql.Date sqlDate = new java.sql.Date(utilDate.getTime());

    }


  def formatGender(ctfId: String, gender: String): String = {
    var rt = "未知"
      if (ctfId.length == 18) {
        try {
          if ((ctfId.substring(16, 17).toInt) % 2 == 0) {
            rt = "M"
          } else {
            rt = "F"
          }
          useRec.add(1)
        } catch {
          case _=> println("error ctfid is : " + ctfId)
          uselessRec.add(1)
        }
      } else {
        uselessRec.add(1)
      }
    rt
  }

  def toInt(s: String): Int = {
    try {
      s.toInt
    } catch {
      case e: Exception => 9999
    }
  }

  def formatCln(birthday: String): String = {
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

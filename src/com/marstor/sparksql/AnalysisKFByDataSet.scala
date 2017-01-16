package com.marstor.sparksql

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SaveMode
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

/**
  * Created by root on 11/25/16.
  */
//case class KFCustomer(name: String, gender: String, ctfId: String, birthday: String, address: String,constellation:String,mobile:String,nation:String,arriveTime:Timestamp)

//case class KFCustomer(name: String, gender: String, ctfId: String, birthday: String,
//                      address: String,constellation:String,mobile:String,nation:String,arriveTime: Timestamp)
case class KFC(name: String, gender: String, ctfId: String, birthday: String,
                      address: String, constellation: String)
case class Location(code: String, description : String)


object AnalysisKFByDataSet {

  val filePath = "/home/panda/2000W/1205/*.csv"
  val locationPath = "/home/panda/2000W/1205/province_city_country.txt"
  val parquetfilePath = "hdfs://192.168.2.190:8020/test/spark/data/kfc"
  val conf = new SparkConf().setAppName("AnalysisKF").setMaster("local[3]")
  val sc = new SparkContext(conf)
  val useRec = sc.accumulator(0, "useful Records")
  val uselessRec = sc.accumulator(0, "useless Records")

  def main(args: Array[String]) {
//    writeData2Parquet()
//    readDataFromParquet()
    analysisKfcData()

  }

  def analysisKfcData(): Unit ={

    sc.setLogLevel("ERROR")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val kfc = sc.textFile(filePath).map(_.split(","))
      .filter(line => line.length > 7)
      .filter(!_.contains("Name"))
      .map(
        p => KFC(p(0), formatGender(p(4), p(5)), p(4), p(4).substring(6, 14), p(7), formatCln(p(4).substring(6, 14)))).toDF()

    val location = sc.textFile(locationPath).map(_.split("\t")).map(p => Location(p(0),p(1))).toDF()

    val kfc1 = sqlContext.read.parquet(filePath)
    val location1 = sqlContext.read.parquet(locationPath)

//    kfc1.filter(" gender = 1").groupBy().join(location1,kfc1("ctfId") == = location1("country"))

    println(location.show(3))

    kfc.registerTempTable("kfc")
    location.registerTempTable("location")
    println("before time:"+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()))
//    查询开房数前10的县/区，耗时3'10"
//    sqlContext.sql("select count(*) amount,substring(k.ctfId,1,6),l.description from kfc k " +
//      "left join location l on substring(k.ctfId,1,6)=l.code group by substring(k.ctfId,1,6),l.description  order by amount desc limit 10 ").collect().foreach(println)
//  kfc.filter(" gender =  'F' ").join(location,(kfc("ctfId")).substr(1,6) == location("country"))

//    kfc.filter(" gender = 'F' ").groupBy(kfc("ctfId").substr(6,4)).agg(max(kfc("local")))


    //    kfc.filter("")
//      .join(location,(kfc("ctfId")).substr(1,6) == location("country")).

//    kfc.filter("gender = 'F' ")
//      .join(location, kfc("ctfId").substr(1,6) == location("country"))
//        .groupBy(location("country"))
//      .agg(kfc("ctfId").substr(1,6),location("country"))

//    in 不支持子查询 eg. select * from src where key in(select key from test);
//    支持查询个数 eg. select * from src where key in(1,2,3,4,5);
//    sqlContext.sql("select count(*) amount from kfc k  where substring(k.ctfId,1,6) not in (select * from location) ").collect().foreach(println)

    println("after time:"+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()))

//    println("success....")
  }

  def writeData2Parquet(): Unit ={
    sc.setLogLevel("ERROR")
    //      Logger.getLogger("org").setLevel(Level.ERROR)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._


    //        val customer = sc.textFile(filePath).map(_.split(",")).filter(line => line.length > 7).filter(!_.contains("Name")).map(
    //          p => KFCustomer(p(0), p(5), p(4), p(6), p(7),formatCln(p(6)),p(16),p(20),formatStr2Timpstamp(p(31)))).toDF()

    //        val customer = sc.textFile(filePath).map(_.split(",")).filter(line => line.length > 7).filter(!_.contains("Name")).map(
    //          p => KFCustomer(p(0), formatGender(p(4),p(5)), p(4), p(4).substring(6,14), p(7),formatCln(p(4).substring(6,14)))).toDF()

    val customer = sc.textFile(filePath).map(_.split(","))
      .filter(line => line.length > 7)
      .filter(!_.contains("Name"))
      .map(
        p => KFCustomer(p(0), formatGender(p(4), p(5)), p(4), p(4).substring(6, 14), p(7), formatCln(p(4).substring(6, 14)))).toDF()
    //    customer.printSchema()
    customer.registerTempTable("customer")

    customer.show(10)
    println(customer.count())
    //    路径需要是不存在的路径
    customer.write.format("parquet").save(parquetfilePath)

    println("parquet success....")
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
  def readDataFromParquet(): Unit ={
    sc.setLogLevel("ERROR")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._
    val parquetLogs = sqlContext.read.parquet(parquetfilePath)
    parquetLogs.registerTempTable("kfc")
        parquetLogs.show(2)

//    sqlContext.sql("select substring(ctfId,1,6),count(*) ccount from kfc group by substring(ctfId,1,6) order by ccount desc ").collect().foreach(println)
//    val sql = "select substring(ctfId,1,6) pcp ,count(*) amount from kfc group by substring(ctfId,1,6) order by amount desc"
//
//    val MYSQL_USERNAME = "root"
//    val MYSQL_PASSWORD = "12345678"
//    val MYSQL_DRIVER = "com.mysql.jdbc.Driver"
//    val MYSQL_URLS = "jdbc:mysql://192.168.2.190:3306/spark_dbtest?useUnicode=true&characterEncoding=utf8"
//    val TABLE_NAME = "kfc_province_city_country"
//    val prop = new java.util.Properties()
//    prop.setProperty("user",MYSQL_USERNAME)
//    prop.setProperty("password",MYSQL_PASSWORD)
//    prop.setProperty("driver",MYSQL_DRIVER)
//
//    sqlContext.sql(sql).write.mode(SaveMode.Append).jdbc(MYSQL_URLS,TABLE_NAME,prop)

    println("mysql success")

  }

  def formatStr2Timpstamp(str: String): java.sql.Timestamp = {
    //    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(str)
    if (str != "Version") {
      new Timestamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(str).getTime)
    } else {
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
        case _ => println("error ctfid is : " + ctfId)
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

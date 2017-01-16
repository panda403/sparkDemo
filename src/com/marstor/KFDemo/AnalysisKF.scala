package com.marstor.KFDemo

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import com.marstor.base.LogLevel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

/**
  * Created by root on 11/25/16.
  */
//case class KFCustomer(name: String, gender: String, ctfId: String, birthday: String, address: String,constellation:String,mobile:String,nation:String,arriveTime:Timestamp)

//case class KFCustomer(name: String, gender: String, ctfId: String, birthday: String,
//                      address: String,constellation:String,mobile:String,nation:String,arriveTime: Timestamp)
case class KFCustomer(name: String, gender: String, ctfId: String, birthday: String,
                      address: String, constellation: String)

case class pcc(id: String, province: String, city: String, country: String, location: String)

object AnalysisKF {

  val kfFilePath = "/home/panda/2000W/1229/qualified.csv"
  val pccPath = "/home/panda/2000W/1229/province_city_country.txt"

  val parquetFilePath = "/home/panda/2000W/1229/parquetfile"

//  val kfFilePath = "hdfs://192.168.2.190:8020/test/spark/data/kf/qualified.csv"
//  val pccPath = "hdfs://192.168.2.190:8020/test/spark/data/kf/province_city_country.txt"


  val conf = new SparkConf().setAppName("AnalysisKF").setMaster("local[3]")
  val sc = new SparkContext(conf)

  val useRec = sc.accumulator(0, "useful Records")
  val uselessRec = sc.accumulator(0, "useless Records")

  def main(args: Array[String]) {
//    usingSQL()
//    readAndWrite()
    AnalysisData()

//    usingBoardCast()
    //    var result = sqlContext.sql("select count(*),case length(ctfId) when 18 then 'use' else 'useless' end  dtype from customer group by dtype")
    //            result.collect().foreach(println)


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

  def readAndWrite(): Unit ={
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    val cdf = sc.textFile(kfFilePath,200).map(_.split(","))
      .filter(line => line.length > 7)
      .filter(!_.contains("Name"))
      .map(
        p => KFCustomer(p(0), formatGender(p(4), p(5)), p(4), getBirthDay(p(4)), p(7), formatCln(p(6)) ) ).toDF()

    cdf.registerTempTable("kf")
//    sqlContext.table("kf").groupBy("constellation").agg("constellation").collect()


//    cdf.write.format("parquet").mode("append").partitionBy("constellation").saveAsTable("parquetKF")
//
//    sqlContext.table("parquetKF").show(2)

    println("success")

  }
  def usingBoardCast(): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val customerRdd = sc.textFile(kfFilePath,200).map(_.split(","))
      .filter(line => line.length > 7)
      .filter(!_.contains("Name"))
      .map(
        p => KFCustomer(p(0), formatGender(p(4), p(5)), p(4), getBirthDay(p(4)), p(7), formatCln(p(6))))

    val pccRdd = sc.textFile(pccPath,5).map(_.split(","))
      .map(q => pcc(q(0), q(1), q(2), q(3), q(4)))

    val pccRddBroadcast = sc.broadcast(pccRdd.collect())

    pccRddBroadcast.value.foreach(println)


  }

  def AnalysisData(): Unit ={

    val startTime = System.currentTimeMillis();

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val customerRdd = sc.textFile(kfFilePath,200).map(_.split(","))
      .filter(line => line.length > 7)
      .filter(!_.contains("Name"))
      .map(
        p => KFCustomer(p(0), formatGender(p(4), p(5)), p(4), getBirthDay(p(4)), p(7), formatCln(p(4)) ))

    val pccRdd = sc.textFile(pccPath).map(_.split(","))
      .map(q => pcc(q(0), q(1), q(2), q(3), q(4)))

    import sqlContext.implicits._

    val customerDF = customerRdd.toDF()
    val pccDF = pccRdd.toDF()
    customerDF.registerTempTable("kfc")
    pccDF.registerTempTable("pcc")
//
//    val kfc_sql = "select count(*) a,substring(ctfId,1,6) province from kfc group by substring(ctfId,1,6) order by a desc "
//    val kfc_sql = "select count(*) a,substring(ctfId,1,2),location from kfc left join pcc on substring(ctfId,1,2)= province group by substring(ctfId,1,2),location order by a desc"
//    val kfc_sql = "SELECT count(constellation) ccount , constellation FROM kfc group by constellation order by ccount desc"
//    val kfc_sql = "select count(*) a,substring(name,1,1) name from kfc group by substring(name,1,1) order by a desc"
//    val kfc_sql = "select count(*) a,gender from kfc group by gender"
//    val kfc_sql = "select * from kfc" 2038'
//    val kfc_sql = "select count(*) a ,constellation,gender from kfc group by constellation,gender order by a desc "//130'
//    val kfc_sql = "select count(*) a,substring(ctfId,7,4),gender from kfc group by substring(ctfId,7,4), gender order by a desc" //162s

    val kfc_sql = "select ctfId ,name,address,count(*) ccount from kfc group by ctfId,name,address order by ccount desc limit 50"
    val table_name = "kfc_ctfId"


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
  }
  def usingSQL()={
    println("*******************start usingSQL....*****************************")
//    Logger.getLogger("org").setLevel(Level.ERROR)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val customerRdd = sc.textFile(kfFilePath,200).map(_.split(","))
      .filter(line => line.length > 7)
      .filter(!_.contains("Name"))
      .map(
        p => KFCustomer(p(0), formatGender(p(4), p(5)), p(4), getBirthDay(p(4)), p(7), formatCln(p(4)) ) )

    val pccRdd = sc.textFile(pccPath).map(_.split(","))
      .map(q => pcc(q(0), q(1), q(2), q(3), q(4)))

    import sqlContext.implicits._

    val customerDF = customerRdd.toDF()
    val pccDF = pccRdd.toDF()
    customerDF.registerTempTable("customer")
    pccDF.registerTempTable("pcc")

    println("************************************************")
    println(useRec.name.get + " " + useRec.value)

    println(uselessRec.name.get + " " + uselessRec.value)
    println("************************************************")

    customerDF.printSchema()
    customerDF.show(2)

    /**
      * 36 minutes
      * [2248272,null]
        [88875,北京市海淀区]
        [88851,上海市扬浦区]
        [70564,上海市虹口区]
        [70191,上海市徐汇区]
        [63079,湖北省武汉市武昌区]
        [61298,上海市普陀区]
        [61152,北京市朝阳区]
        [57018,上海市浦东新区]
        [53651,上海市闸北区]
      */
      println("start time:"+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()))
        val result = sqlContext.sql("select count(*) ac ,p.location from customer c left join pcc p on substring(c.ctfId,0,6) = p.country group by p.location order by ac desc  limit 10")
        result.collect().foreach(println)
      println("end time:"+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()))
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

  def getHour(kftime: String): String ={
    try {
      if (kftime.length < 10) {
        "25"
      } else {
        kftime.split(" ")(1).split(":")(0)
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

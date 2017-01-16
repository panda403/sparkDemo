package com.marstor.sparksql

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.sql.SaveMode
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
case class KFCustomer2(name: String, gender: String, ctfId: String, birthday: String,
                      address: String, constellation: String)

object AnalysisKF2 {

  val filePath = "/home/panda/2000W/1000W-1200W.csv"
  val conf = new SparkConf().setAppName("AnalysisKF").setMaster("local[1]")
  val sc = new SparkContext(conf)
  val useRec = sc.accumulator(0, "useful Records")
  val uselessRec = sc.accumulator(0, "useless Records")

//  val useRec = sc.broadcast(sc.accumulator(0, "useful Records"))
//  val uselessRec = sc.broadcast(sc.accumulator(0, "useless Records"))

  def main(args: Array[String]) {


    sc.setLogLevel("ERROR")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._


    val customer = sc.textFile(filePath).map(_.split(",")).filter(line => line.length > 7).filter(!_.contains("Name")).map(
      p => KFCustomer2(p(0), formatGender(p(4), p(5)), p(4), p(6), p(7), p(6))).toDF()

//    customer.collect()
//
    customer.registerTempTable("customer")
    var result = sqlContext.sql("SELECT gender, count(gender) FROM customer  group by gender")
    result.collect().foreach(println)

//    customer.registerTempTable("customer")
//    var result = sqlContext.sql("SELECT gender, count(gender) FROM customer where gender = 'F' or gender = 'M' group by gender")
//    result.collect().foreach(println)

//    Thread.sleep(10000)

    println("************************************************")
    println(useRec.name.get + " " + useRec.value)

    println(uselessRec.name.get + " " + uselessRec.value)
    println("************************************************")


  }

  def formatGender(ctfId: String, gender: String): String = {
    var rt = "未知"
    if (gender.length != 1) {
      if (ctfId.length == 18) {
        try {
          if ((ctfId.substring(16, 17).toInt) % 2 == 0) {
            rt = "M"
          } else {
            rt = "F"
          }
          useRec.add(1)
        } catch {
          case e=> println("error ctfid is : " + ctfId)
          uselessRec.add(1)
        }
      } else {
        uselessRec.add(1)
      }
    } else {
      useRec.add(1)
      rt = gender
    }
    rt
  }

}

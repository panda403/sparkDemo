package com.marstor.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

/**
  * Created by root on 10/20/16.
  */
case class people(id: Integer, gender: String, height: Integer)

object PeopleDataStatistics2 {

  def main(args: Array[String]) {
    val filePath = "hdfs://192.168.2.190:8020/test/spark/data/sample_people_info.txt"

    val conf = new SparkConf().setAppName("Spark Exercise:People Data Statistics 2").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    val peopleDF = sc.textFile(filePath).map(_.split(" ")).map(p => people(Integer.parseInt(p(0)), p(1), Integer.parseInt(p(2)))).toDF()

    peopleDF.registerTempTable("people")

    peopleDF.select("id", "gender", "height").show(5)

//    //get the male people whose height is more than 180
//    val higherMale180 = sqlContext.sql("select id,gender,height from people where height > 180 and gender = 'M' ")
//    println("Men whose height are more than 180: " + higherMale180.count())
//    println("<Display #1>")
//    //get the female people whose height is more than 170
//    val higherFemale170 = sqlContext.sql("select id,gender,  height from people where height > 170 and gender = 'F' ")
//    println("Women whose height are more than 170: " + higherFemale170.count())
//    println("<Display #2>")
//    //Grouped the people by gender and count the number
//    peopleDF.groupBy(peopleDF("gender")).count().show()
//    println("People Count Grouped By Gender")
//    println("<Display #3>")
//    //
//    peopleDF.filter(peopleDF("gender").equalTo("M")).filter(
//      peopleDF("height") > 210).show(50)
//    println("Men whose height is more than 210")
//    println("<Display #4>")
//    //
//    peopleDF.sort($"height".desc).take(50).foreach { row => println(row(0) + "," + row(1) + "," + row(2)) }
//    println("Sorted the people by height in descend order,Show top 50 people")
//    println("<Display #5>")
//    //
//    peopleDF.filter(peopleDF("gender").equalTo("M")).agg(Map("height" -> "avg")).show()
//    println("The Average height for Men")
//    println("<Display #6>")
//    //
//    peopleDF.filter(peopleDF("gender").equalTo("F")).agg("height" -> "max").show()
//    println("The Max height for Women:")
//    println("<Display #7>")
//    //......
//    println("All the statistics actions are finished on structured People data.")


    println("**************start inset into mysql*****************************")

    val MYSQL_USERNAME = "root"
    val MYSQL_PASSWORD = "12345678"
    val MYSQL_DRIVER = "com.mysql.jdbc.Driver"
    val MYSQL_URLS = "jdbc:mysql://192.168.2.190:3306/spark_dbtest"
    val TABLE_NAME = "people"
    val prop = new java.util.Properties()
    prop.setProperty("user", MYSQL_USERNAME)
    prop.setProperty("password", MYSQL_PASSWORD)
    prop.setProperty("driver", MYSQL_DRIVER)

    sqlContext.sql("select id,gender,height from  people order by height desc").write.jdbc(MYSQL_URLS, TABLE_NAME, prop)

    println("mysql success")


  }

}

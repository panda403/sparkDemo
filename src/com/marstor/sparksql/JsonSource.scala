package com.marstor.sparksql

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 12/14/16.
  */
object JsonSource {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AnalysisKF").setMaster("local[3]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val student_course_path = "/home/panda/json/student_course.json"
    val course_path = "/home/panda/json/course.json"
    val student_path = "/home/panda/json/student.json"

    sqlContext.sql("""create table student_course using org.apache.spark.sql.json options(path "/home/panda/json/student_course.json")""")
    sqlContext.sql("""create table course using org.apache.spark.sql.json options(path "/home/panda/json/course.json")""")
    sqlContext.sql("""create table student using org.apache.spark.sql.json options(path "/home/panda/json/student.json")""")
    sqlContext.sql("show tables").show()
    sqlContext.sql("select * from course c,student s,student_course sc where c.cid=sc.cid and s.sid=sc.sid").show()
  }
}

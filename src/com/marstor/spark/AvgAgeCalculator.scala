package com.marstor.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * Created by root on 10/13/16.
  */
object AvgAgeCalculator {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
//    val filePath = "/panda/sparkdata/sample_age_data.txt"
      val filePath = "hdfs://192.168.2.190:8020/test/spark/data/sample_age_data.txt"
    /*if (args.length < 1) {
      println("Usage:AvgAgeCalculator datafile")
      System.exit(1)
    }*/
    val conf = new SparkConf().setAppName("Spark Exercise:Average Age Calculator").setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val dataFile = sc.textFile(filePath, 5);
    val count = dataFile.count()
    val ageData = dataFile.map(line => line.split(" ")(1))
    val totalAge = ageData.map(age => Integer.parseInt(
      String.valueOf(age))).collect().reduce((a, b) => a + b)
    println("Total Age:" + totalAge + ";Number of People:" + count)
    val avgAge: Double = totalAge.toDouble / count.toDouble
    println("Average Age is " + avgAge)
  }

}

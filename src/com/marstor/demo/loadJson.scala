package com.marstor.demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 7/15/16.
  */
object loadJson {
  def main(argment: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("loadJson").setMaster("local") //本地调试运行
    val sc = new SparkContext(conf) // 建立spark操作上下
    // sc is an existing SparkContext.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // A JSON dataset is pointed to by path.
    // The path can be either a single text file or a directory storing text files.
    val path = "resources/people.json"
    val people = sqlContext.read.json(path)

    // The inferred schema can be visualized using the printSchema() method.
    people.printSchema()
    // root
    //  |-- age: integer (nullable = true)
    //  |-- name: string (nullable = true)

    // Register this DataFrame as a table.
    people.registerTempTable("people")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

    // Alternatively, a DataFrame can be created for a JSON dataset represented by
    // an RDD[String] storing one JSON object per string.
    val anotherPeopleRDD = sc.parallelize(
      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val anotherPeople = sqlContext.read.json(anotherPeopleRDD)
  }

}

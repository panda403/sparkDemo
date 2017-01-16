package com.marstor.demo

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 7/13/16.
  *
  * {"name":"Michael"}
  * {"name":"Andy", "age":30}
  * {"name":"Justin", "age":19}
  *
  */
object CreateDataFrames {
  def main(argment: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("createDFDemo").setMaster("local") //本地调试运行
    val sc = new SparkContext(conf) // 建立spark操作上下文
    val sqlContext = new SQLContext(sc) // 建立spark sql 操作上下文

   //val df = sqlContext.read.json("/panda/spark/sparkData/sparksql/people.json")
    val df = sqlContext.read.json("/panda/spark/sparkData/testfile.log")


    // Displays the content of the DataFrame to stdout
    //df.show()


    df.printSchema()
    //df.select("name").show()
    //df.select(df("name"), df("age") + 1).show()
    //df.filter(df("age") > 21).show()
    //df.groupBy("age").count().show()



  }
}

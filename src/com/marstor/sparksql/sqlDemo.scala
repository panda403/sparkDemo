package com.marstor.sparksql

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

/*
* resolve people1.txt,usefull*/
import scala.collection.mutable

object sqlDemo {
  def main(argment: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sqlDemo").setMaster("local") //本地调试运行
    val sc = new SparkContext(conf)  // 建立spark操作上下文
    val sqlContext = new SQLContext(sc) // 建立spark sql 操作上下文

    val rowRDD = sc.textFile("hdfs://192.168.0.211:8020/test/sparksqltest/data/testfile.log")   //读取 csv 文件
      .map(line => {                                  // 转换成 row 结构
    val data = line.split("\t")                    // 切分字段
    val list = mutable.ArrayBuffer[Any]()
      list.append(data(0))
      list.append(data(1))
      list.append(data(2))
      list.append(data(3))
      Row.fromSeq(list)                           //创建row
    })
//    System.nanoTime()

    val fields = new mutable.ArrayBuffer[StructField]()
    fields.append(StructField("times",DataTypes.CalendarIntervalType,true))
    /*fields.append(StructField("times",TimestampType,true))*/
    fields.append(StructField("user",StringType,true))
    fields.append(StructField("ip",StringType,true))
    fields.append(StructField("desc",StringType,true))
    val schema = StructType(fields)

    val rdd = sqlContext.applySchema(rowRDD, schema)

    rdd.registerTempTable("testfile")


    /*sqlContext.sql("select * from testfile limit 10 ")
      .collect()
      .foreach(println)*/
    sqlContext.sql("select count(*) from testfile where ip = '192.168.0.24' ")
      .collect()
      .foreach(println)

    /*sqlContext.sql("select count(*) from testfile where times > 2015 ")
      .collect()
      .foreach(println)*/

    sc.stop()
  }
}

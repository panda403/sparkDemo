package com.marstor.sparksql

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by root on 11/9/16.
  */
case class Record(key: Int, value: String)
object SparkHiveExample {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HiveFromSpark").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    import hiveContext.sql

    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    sql(s"LOAD DATA LOCAL INPATH '/panda/spark/data/kv1.txt' INTO TABLE src")

    // Queries are expressed in HiveQL
    println("Result of 'SELECT *': ")
    sql("SELECT * FROM src").collect().foreach(println)

    // Aggregation queries are also supported.
    val count = sql("SELECT COUNT(*) FROM src").collect().head.getLong(0)
    println(s"COUNT(*): $count")

    // The results of SQL queries are themselves RDDs and support all normal RDD functions.  The
    // items in the RDD are of type Row, which allows you to access each column by ordinal.
    val rddFromSql = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

    println("Result of RDD.map:")
    val rddAsStrings = rddFromSql.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }

    // You can also register RDDs as temporary tables within a HiveContext.
    val rdd = sc.parallelize((1 to 100).map(i => Record(i, s"val_$i")))
    rdd.toDF().registerTempTable("records")

    // Queries can then join RDD data with data stored in Hive.
    println("Result of SELECT *:")
    sql("SELECT * FROM records r JOIN src s ON r.key = s.key").collect().foreach(println)

    sc.stop()
  }
}

package com.marstor.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.sum

/**
  * Created by root on 12/15/16.
  */
object AggregateTest {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("AggregateTest").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val df = sc.parallelize(Seq(
      (1.0, 0.3, 1.0), (1.0, 0.5, 0.0),
      (-1.0, 0.6, 0.5), (-1.0, 5.6, 0.2))
    ).toDF("col1", "col2", "col3")

//    df.groupBy($"col1").min().show

//    +----+---------+---------+---------+
//    |col1|min(col1)|min(col2)|min(col3)|
//      +----+---------+---------+---------+
//    | 1.0|      1.0|      0.3|      0.0|
//      |-1.0|     -1.0|      0.6|      0.2|
//      +----+---------+---------+---------+

//    df.groupBy("col1").sum("col2", "col3").show()

//    +----+-----------------+---------+
//    |col1|        sum(col2)|sum(col3)|
//      +----+-----------------+---------+
//    | 1.0|              0.8|      1.0|
//      |-1.0|6.199999999999999|      0.7|
//      +----+-----------------+---------+

//    val exprs = df.columns.map((_ -> "mean")).toMap
//    df.groupBy($"col1").agg(exprs).show()

//    +----+---------+------------------+---------+
//    |col1|avg(col1)|         avg(col2)|avg(col3)|
//      +----+---------+------------------+---------+
//    | 1.0|      1.0|               0.4|      0.5|
//      |-1.0|     -1.0|3.0999999999999996|     0.35|
//      +----+---------+------------------+---------+


    val exprs = df.columns.map(sum(_))
    df.groupBy($"col1").agg(exprs.head, exprs.tail: _*).show()

//    +----+---------+-----------------+---------+
//    |col1|sum(col1)|        sum(col2)|sum(col3)|
//      +----+---------+-----------------+---------+
//    | 1.0|      2.0|              0.8|      1.0|
//      |-1.0|     -2.0|6.199999999999999|      0.7|
//      +----+---------+-----------------+---------+

  }

}

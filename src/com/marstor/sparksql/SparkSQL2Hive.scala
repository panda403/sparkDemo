package com.marstor.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by root on 11/9/16.
  */

/**
  * 通过spark sql操作hive数据源
  */
object SparkSQL2Hive {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf();
    conf.setAppName("SparkSQL2Hive for scala")
    //    conf.setMaster("spark://master:7077")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val hiveContext = new HiveContext(sc)
    //用户年龄
    //    hiveContext.sql("use panda")
    hiveContext.sql("DROP TABLE IF EXISTS people")
    //    hiveContext.sql("CREATE TABLE IF NOT EXISTS people(name STRING, age INT)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'")
    hiveContext.sql("CREATE TABLE IF NOT EXISTS people(name STRING, age INT)")
    //把本地数据加载到hive中（实际上发生了数据拷贝），也可以直接使用HDFS中的数据
    hiveContext.sql(s"LOAD DATA LOCAL INPATH '/panda/spark/data/hive/people.txt' INTO TABLE people")
    hiveContext.sql("select * from people").collect().foreach(println)

    //用户份数
    //    hiveContext.sql("use panda")
    hiveContext.sql("DROP TABLE IF EXISTS peopleScores")
    hiveContext.sql("CREATE TABLE IF NOT EXISTS peopleScores(name STRING, score INT)")
    hiveContext.sql(s"LOAD DATA LOCAL INPATH '/panda/spark/data/hive/peopleScore.txt' INTO TABLE peopleScores")
    hiveContext.sql("select * from peopleScores").collect().foreach(println)

    /**
      * 通过HiveContext使用join直接基于hive中的两种表进行操作
      */
    val resultDF = hiveContext.sql("select pi.name,pi.age,ps.score "
      + " from people pi join peopleScores ps on pi.name=ps.name"
      + " where ps.score>90");

    resultDF.collect().foreach(println)

    /**
      * 通过saveAsTable创建一张hive managed table，数据的元数据和数据即将放的具体位置都是由
      * hive数据仓库进行管理的，当删除该表的时候，数据也会一起被删除（磁盘的数据不再存在）
      */
    hiveContext.sql("drop table if exists peopleResult")
    resultDF.saveAsTable("peopleResult")

    /**
      * 使用HiveContext的table方法可以直接读取hive数据仓库的Table并生成DataFrame,
      * 接下来机器学习、图计算、各种复杂的ETL等操作
      */
    val dataframeHive = hiveContext.table("peopleResult")
    dataframeHive.show()


  }
}

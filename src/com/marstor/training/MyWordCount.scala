package com.marstor.training

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 12/26/16.
  */
object MyWordCount{
  def main(args: Array[String]) {
    val sparkconf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(sparkconf)

    val fileRdd = sc.textFile("/home/panda/wordcount.txt")

    val wordCountRdd = fileRdd.flatMap(line => line.split(" ")).map(x => (x,1)).reduceByKey(_ + _)

    wordCountRdd.saveAsTextFile("/home/panda/wordcountresult.txt")

    wordCountRdd.foreach(println)
  }
}

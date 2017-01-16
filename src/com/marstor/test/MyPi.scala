package com.marstor.test

import org.apache.spark.{SparkConf, SparkContext}

import scala.math.random

/**
  * Created by root on 12/28/16.
  */
object MyPi {

  def main(args: Array[String]) {

    val sparkconf = new SparkConf().setAppName("pi").setMaster("local[2]")
    val sc = new SparkContext(sparkconf)

    val slices = 10
    val n = 10000000 * slices

    val count = sc.parallelize(1 to n,slices).map{ i =>
      val x = random * 2 -1
      val y = random * 2 -1
      if((x*x + y*y) < 1)1 else 0
    }.reduce(_ + _)

    println("pi is roughly : " + 4.0*count/n)

  }

}

package com.marstor.base

/**
  * Created by root on 1/9/17.
  */
object Utils {
  def main(args: Array[String]) {
    splitTest()
  }
  def splitTest(): Unit ={
//    val str = "2012-1-11 14:27:46"
    val str = "2012-1-16 7:05:03"
    print(str.split(" ")(1).split(":")(0))
  }

}

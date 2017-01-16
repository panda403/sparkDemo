package com.marstor.base

/**
  * Created by root on 1/9/17.
  */
object TimeDiffer {
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis();
    Thread.sleep(10*1000)
    val endTime = System.currentTimeMillis();
    println("the differ time is :"+(endTime-startTime)/1000)
  }
}

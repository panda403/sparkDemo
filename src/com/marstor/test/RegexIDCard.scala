package com.marstor.test

/**
  * Created by root on 12/5/16.
  */
object RegexIDCard {
  def main(args: Array[String]) {
    val pattern2 = "^(\\d{15}$|^\\d{18}$|^\\d{17}(\\d|X|x))$"
    val pattern = "^[1-9]\\d{7}((0\\d)|(1[0-2]))(([0|1|2]\\d)|3[0-1])\\d{3}$)|(^[1-9]\\d{5}[1-9]\\d{3}((0\\d)|(1[0-2]))(([0|1|2]\\d)|3[0-1])((\\d{4})|\\d{3}[Xx])$)$"
    val log: String = "吕敢,,,ID,420302199211130912,M,19921113,,,F,,,,,,,,,,18672383748,,,465987452@qq.com,01,,,,,,,0,2013-1-9 8:28:54,20042769"
    println(log.split(",")(4))

    if(log.split(",")(4).matches(pattern)){
      println("----------------"+ log + "--------------")
    }else{
      println("*****************"+log)
    }
  }
}

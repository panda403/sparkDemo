package com.marstor.test

/**
  * Created by root on 11/1/16.
  */
object ScalaExtractor {

  def match_arr(arr: Any) = arr match {
    //判断arr是数组，并且只有一个元素，该值为0的规则
    case Array(0) => println("array:" + 0)
    //判断arr是数组，并且只有2个元素的规则，提取出，x和y
    case Array(x, y) => println("array: x=" + x + ",y=" + y)
    //判断arr是数组，并且多余一个元素，且第一个元素为0的规则
    case Array(0, _*) => println("array:0-------------")
    case _ => println("other") //判断arr是数组
  }

  def main(args: Array[String]) {
//    match_arr(Array(0))
//    match_arr(Array(0, 1))
//    match_arr((Array(0, 1, 2, 3)))
//    match_arr(Array())

    val pattern = "(\\d+) (\\w+)".r //定义正则
    "1234 abc" match {
      case pattern(number, chars) => println( number +" " + chars)
    }



  }
}

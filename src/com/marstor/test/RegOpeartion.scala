package com.marstor.test

/**
  * Created by root on 11/1/16.
  */
object RegOpeartion {
  def main(args: Array[String]): Unit = {
    val regex ="""([0-9]+) ([a-z]+)""".r //正则表达式匹配模式
    val numP = "[0-9]+".r
    val numberP ="""\s+[0-9]+\s+""".r
    println(regex + "   " + numP + "    " + numberP)

    for (matchString <- numP.findAllIn("99958 scala,65464 spark"))
      println(matchString) //找出所有匹配的

    println(numberP.findFirstIn("99ss java ,456464 hadoop")) //找出第一个匹配得

    val numitemP ="""([0-9]+) ([a-z]+)""".r
    val numitemP(num, item) = "99 hadoop" //分开匹配
    val line = "4654 spark"
    line match {
      //输出匹配的
      case numitemP(num, item) => println(num + "\t" + item)
      case _ => println("error")
    }

  }
}

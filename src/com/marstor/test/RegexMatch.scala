package com.marstor.test

/**
  * Created by root on 11/1/16.
  */
object RegexMatch {
  def main(args: Array[String]) {
    val sparkRegex = "^[\\w-]+(\\.[\\w-]+)*@[\\w-]+(\\.[\\w-]+)+$".r
    for (matchString <- sparkRegex.findAllIn("zhouzhihubeyond@sina.com")) {
      println(matchString)
    }


  }
}

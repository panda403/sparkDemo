package com.marstor.test

/**
  * Created by root on 11/29/16.
  */
object str2int {
  def main(args: Array[String]) {
    var rt = "未知"
    val str = "320902196902160526"
    val gi = str.substring(16,17).toInt
    if(str.substring(16,17).toInt % 2 == 0){
      rt = "M"
    }else{
      rt = "F"
    }
    println(rt)
  }

}

package com.marstor.KFDemo

import java.io.{File, FileOutputStream, PrintWriter}

import scala.io.Source
import scala.util.control.Breaks

/**
  * Created by root on 12/2/16.
  */
object CleanData {
  var unQualifiedDataCount = 0
  var errorCtfId = 0

  val filePath = "/home/panda/2000W/new/last5000.csv"
  val newFilePath = "/home/panda/2000W/1229/qualified.csv"
  val under18DataPath = "/home/panda/2000W/1229/under18.csv"
  val errorCtfIdPath = "/home/panda/2000W/1229/errorCtfId.csv"

//  val writer = new PrintWriter(new File(newFilePath)) //不追加写文件
  val writer =  new PrintWriter(new FileOutputStream(new File(newFilePath),true))  //追加写文件
  val under18Writer =  new PrintWriter(new FileOutputStream(new File(under18DataPath),true))
  val errorCtfIdWriter = new PrintWriter(new FileOutputStream(new File(errorCtfIdPath),true))
  def main(args: Array[String]) {

//    readFile(filePath)
    dealFile(filePath,newFilePath)
//    showUselessData(filePath)



  }
  def showUselessData(filePath : String): Unit ={
    for(line <- Source.fromFile(filePath).getLines()){
      if(line.split(",").length < 17){
        println(line)
      }
    }
  }

  def dealFile(filePath : String,newFilePath: String): Unit ={
    for(line <- Source.fromFile(filePath).getLines()){
      if(checkLine(line)){
        writer.write(line)
        writer.write(System.lineSeparator())
      }
    }
    writer.close()
    println(unQualifiedDataCount+"**************"+errorCtfId)
    println("success....")
  }

  def checkLine(line : String): Boolean ={

    var result = false
//    val pattern = "^[1-9]\\d{5}[1-9]\\d{3}((0\\d)|(1[0-2]))(([0|1|2]\\d)|3[0-1])\\d{4}$".r
    val pattern = "^(\\d{15}$|^\\d{18}$|^\\d{17}(\\d|X|x))$"

    try {
      if ((line.length > 7) && (line.split(",")(4).length == 18) && line.split(",")(4).matches(pattern)) {//校验身份证位数
        if(checkCtfID(line)){
          result = true
        }
      }else{
        println("*********位数不合格的身份证号是:"+line)
        unQualifiedDataCount += 1
        under18Writer.write(line)
        under18Writer.write(System.lineSeparator())
      }
    } catch {
      case e => "error line:" + line
    }
    result
  }

  def checkCtfID(line : String): Boolean ={
    val ctfId = line.split(",")(4)
    var valideAddress = false;
    val provinceArray = Array( "11:北京", "12:天津", "13:河北", "14:山西",
      "15:内蒙古", "21:辽宁", "22:吉林", "23:黑龙江", "31:上海", "32:江苏",
      "33:浙江", "34:安徽", "35:福建", "36:江西", "37:山东", "41:河南",
      "42:湖北 ", "43:湖南", "44:广东", "45:广西", "46:海南", "50:重庆",
      "51:四川", "52:贵州", "53:云南", "54:西藏 ", "61:陕西", "62:甘肃",
      "63:青海", "64:宁夏", "65:新疆", "71:台湾", "81:香港", "82:澳门",
      "91:国外" )
    val loop = new Breaks;
    loop.breakable {
      for (x <- provinceArray) {
        var provinceKey = x.split(":")(0);
        if (provinceKey.equals(ctfId.substring(0, 2))) {
          valideAddress = true;
          loop.break;
        }
      }
    }
    if(!valideAddress){
      errorCtfId += 1
      println("不合格的身份证号是:"+ctfId)

      errorCtfIdWriter.write(line)
      errorCtfIdWriter.write(System.lineSeparator())
    }
    valideAddress
  }
  def readFile(filePath : String): Unit ={

    println("Following is the content read:")

    Source.fromFile(filePath).foreach {
//    Source.fromFile("/home/panda/2000W/1000W-1200W.csv").foreach {
      print
    }
  }
}

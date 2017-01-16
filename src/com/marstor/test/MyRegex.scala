package com.marstor.test

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
import scala.util.matching.Regex

/**
  * Created by root on 11/2/16.
  */

case class zylog(logTime: String, packageName: String, logLevel: String, logDesc: String)

object MyRegex {
  def main(args: Array[String]) {

//        regexLog()
    //    fileReading("/panda/spark/logs/DSSPlatform-NEW.log.2016-10-09")
        regexFile("/panda/spark/logs/abc.txt")

//    val line = "2016-10-09 05:21:13 [org.springframework.beans.factory.xml.XmlBeanDefinitionReader.loadBeanDefinitions(315)]-[INFO] Loading XML bean definitions from ServletContext resource [/WEB-INF/applicationContext.xml]"
//    val log = MyRegex.parseLogLine(line)
//    println(log.logTime)
//    println(log.packageName)
//    println(log.logLevel)
//    println(log.logDesc)
  }

  def regexLog(): Unit = {
//    val pattern = "(^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}) (\\[.*\\)])(-\\[.*\\]) (.+)".r
      val pattern = "^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}) (\\[.*\\)])(-\\[\\w{4,5}\\]) ([\\s\\S]*)$".r
//    val log: String = "2016-10-09 00:00:00 [com.sinosoft.dds.controller.ProductSearchController.getCurLang(908)]-[INFO] Language Change to : zh_CN"
    val log: String = "2016-10-09 05:21:28 [org.springframework.web.servlet.mvc.annotation.DefaultAnnotationHandlerMapping.registerHandler(411)]-[INFO] Mapped URL path [/collectInfoInEdit.html] onto handler 'collectIntoController' "


    log match {
      case pattern(chars1, chars2, chars3, chars4) => println(chars1 + System.getProperty("line.separator") + chars2 +
        System.getProperty("line.separator") + chars3 + System.getProperty("line.separator") + chars4)
    }
  }

  def fileReading(path: String) {
    import java.io.File
    import scala.io.Source
    val s = Source.fromFile(new File(path)).getLines()
    s.foreach(println(_))
  }

  def regexFile(path: String): Unit = {
    //    val pattern = "(^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}) (\\[.*\\)])(-\\[.*\\w{4,6}\\]) (.+)".r
    val pattern = "^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}) (\\[.*\\)])(-\\[.*\\w{4,6}\\]) ([\\s\\S]*)$".r
    //    val rf = Source.fromFile(new File(path)).getLines()

    val sc = new SparkContext(new SparkConf().setAppName("myregex").setMaster("local[2]"))
    val compatiedAc = sc.accumulator(0,"compatied numbers")
    val unCompatiedAc = sc.accumulator(0,"unCompatied numbers")
//    var compatiedCount = 0
//    var unCompatiedCount = 0
    for (i <- Source.fromFile(new File(path)).getLines()) {
      i match {
        case pattern(logTime, packageName, logLevel, logDesc) => println(logTime + System.getProperty("line.separator") + packageName +
          System.getProperty("line.separator") + logLevel + System.getProperty("line.separator") + logDesc)
//          compatiedCount += 1
          compatiedAc.add(1)
        case _ =>
//          unCompatiedCount += 1
          unCompatiedAc.add(1)
      }
    }
//    println(compatiedCount + "************************************" + unCompatiedCount)
    println(compatiedAc.name.get + " " + compatiedAc.value+System.getProperty("line.separator")+unCompatiedAc.name.get + " " + unCompatiedAc.value)
  }


  def parseLogLine(log: String): zylog = {
    val PATTERN = "^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}) (\\[.*\\)])(-\\[.*\\w{4,6}\\]) (.+)".r
    val res = PATTERN.findFirstMatchIn(log)
    if (res.isEmpty) {
      throw new RuntimeException("Cannot parse log line: " + log)
    }
    val m = res.get
    zylog(m.group(1), m.group(2), m.group(3), m.group(4))
  }
}

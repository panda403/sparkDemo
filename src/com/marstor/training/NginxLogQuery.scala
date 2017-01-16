package com.marstor.training

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 12/28/16.
  */

class Stats(val count: Int, val numBytes: Int) extends Serializable {
  def merge(other: Stats) = new Stats(count + other.count, numBytes + other.numBytes)

  override def toString = "bytes=%s\tn=%s".format(numBytes, count)
}

object NginxLogQuery {
  val apacheLogRegex = """^([\d.]+) (\S+) (\S+) \[([\w\d:/]+\s[+\-­]\d{4})\] "(.+?)" (\d{3}) ([\d\­‐]+) "([^"]+)" "([^"]+)".*""".r
  def main(args: Array[String]) {
    val sparkconf = new SparkConf().setAppName("nginx log query").setMaster("local[2]")
    val sc = new SparkContext(sparkconf)

  }

  def extractKey(line: String): (String, String, String) = {
    apacheLogRegex.findFirstIn(line) match {
      case Some(apacheLogRegex(ip, _, user, dateTime, query, status, bytes, referer, ua))
      =>
        if (user != "\"­‐\"") (ip, user, query)
        else (null, null, null)
      case _ => (null, null, null)
    }
  }

  def extractStats(line: String): Stats = {
    apacheLogRegex.findFirstIn(line) match {
      case Some(apacheLogRegex(ip, _, user, dateTime, query, status, bytes, referer, ua)) => new Stats(1, bytes.toInt)
      case _ => new Stats(1, 0)
    }
  }
}

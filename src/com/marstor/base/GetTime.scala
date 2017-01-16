package com.marstor.base

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.{Calendar, Date}

/**
  * Created by root on 11/3/16.
  */
object GetTime {
  def main(args: Array[String]) {
    println("现在时间：" + GetTime.getNowDate())
    println("昨天时间：" + GetTime.getYesterday())
    println("本周开始" + GetTime.getNowWeekStart())
    println("本周结束" + GetTime.getNowWeekEnd())

    println("本月开始" + GetTime.getNowMonthStart())
    println("本月结束" + GetTime.getNowMonthEnd())

    print("\n")

    println(GetTime.timeFormat("1436457603"))
    println(GetTime.DateFormat("1436457603"))


  }

  def getNowDate(): String = {
    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var hehe = dateFormat.format(now)
    hehe
  }

  def getYesterday(): String = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }

  def getNowWeekStart(): String = {
    var period: String = ""
    var cal: Calendar = Calendar.getInstance();
    var df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    //获取本周一的日期
    period = df.format(cal.getTime())
    period
  }

  def getNowWeekEnd(): String = {
    var period: String = ""
    var cal: Calendar = Calendar.getInstance();
    var df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY); //这种输出的是上个星期周日的日期，因为老外把周日当成第一天
    cal.add(Calendar.WEEK_OF_YEAR, 1) // 增加一个星期，才是我们中国人的本周日的日期
    period = df.format(cal.getTime())
    period
  }

  def getNowMonthStart(): String = {
    var period: String = ""
    var cal: Calendar = Calendar.getInstance();
    var df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    cal.set(Calendar.DATE, 1)
    period = df.format(cal.getTime()) //本月第一天
    period
  }

  def getNowMonthEnd(): String = {
    var period: String = ""
    var cal: Calendar = Calendar.getInstance();
    var df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    cal.set(Calendar.DATE, 1)
    cal.roll(Calendar.DATE, -1)
    period = df.format(cal.getTime()) //本月最后一天
    period
  }

  def DateFormat(time: String): String = {
    var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var date: String = sdf.format(new Date((time.toLong * 1000l)))
    date
  }

  def timeFormat(time: String): String = {
    var sdf: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
    var date: String = sdf.format(new Date((time.toLong * 1000l)))
    date
  }

  //核心工作时间，迟到早退等的的处理
  def getCoreTime(start_time: String, end_Time: String) = {
    var df: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
    var begin: Date = df.parse(start_time)
    var end: Date = df.parse(end_Time)
    var between: Long = (end.getTime() - begin.getTime()) / 1000 //转化成秒
    var hour: Float = between.toFloat / 3600
    var decf: DecimalFormat = new DecimalFormat("#.00")
    decf.format(hour) //格式化

  }
}

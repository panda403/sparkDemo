package com.marstor.test

import java.util.Date

import org.apache.commons.lang.builder.{ReflectionToStringBuilder, ToStringStyle}

/**
  * Created by root on 11/3/16.
  */
class zyLogger extends Serializable {
  var time: Date = null
  var packageName: String = ""
  var logLevel : String = ""
  var msg: String = ""

  def this(time: Date, packageName: String, logLevel: String, msg: String) {
    this()
    this.time = time;
    this.packageName = packageName;
    this.logLevel = logLevel;
    this.msg = msg;
  }
  override def toString = ReflectionToStringBuilder.toString( this ,ToStringStyle.MULTI_LINE_STYLE)

//  def toString() {
//    ReflectionToStringBuilder.toString( this ,ToStringStyle.MULTI_LINE_STYLE);
//    //        return new ToStringBuilder(this,ToStringStyle.MULTI_LINE_STYLE).append("xml", xml).toString();
//  }

}
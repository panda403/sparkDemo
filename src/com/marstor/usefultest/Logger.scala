package com.marstor.usefultest

import java.util.Date

/**
  * Created by root on 11/3/16.
  */
class Logger extends Serializable {
  var time: Date = null
  var thread: String = ""
  var level: String = ""
  var logger: String = ""
  var msg: String = ""

  def this(time: Date, thread: String, level: String, logger: String, msg: String) {
    this()
    this.time = time;
    this.thread = thread;
    this.level = level;
    this.logger = logger;
    this.msg = msg;
  }
}
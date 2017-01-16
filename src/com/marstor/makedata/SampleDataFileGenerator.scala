package com.marstor.makedata

import java.io.FileWriter
import java.io.File
import scala.util.Random

/**
  * Created by root on 10/13/16.
  * data like
  * 1 59
  * 2 36
  */
object SampleDataFileGenerator {
  def main(args: Array[String]) {
    val writer = new FileWriter(new File("/panda/spark/data/sample_age_data.txt"), false)
    val rand = new Random()
    for (i <- 1 to 10000000) {
      writer.write(i + " " + rand.nextInt(100))
      writer.write(System.getProperty("line.separator"))
    }
    println("sample_age_data.txt success.....")
    writer.flush()
    writer.close()
  }
}

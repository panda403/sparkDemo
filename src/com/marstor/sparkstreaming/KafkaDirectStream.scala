package com.marstor.sparkstreaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.log4j.{Level, Logger}

/**
  * Created by root on 10/31/16.
  */
object KafkaDirectStream {

def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("kafka-spark-demo")
    val scc = new StreamingContext(sparkConf, Duration(5000))
    scc.checkpoint(".")
   Logger.getLogger("org").setLevel(Level.ERROR)
    val topics = Set("20160920")
    val kafkaParam = Map(
      //"metadata.broker.list" -> "slave1:9091,slave2:9091"
//    "metadata.broker.list" -> "192.168.2.185:9092,192.168.2.186:9092,192.168.2.187:9092"
    "metadata.broker.list" -> "broker1:9092,broker2:9092,broker3:9092"
    )
    val stream: InputDStream[(String, String)] = createStream(scc, kafkaParam, topics)
    stream.map(_._2)
      .flatMap(_.split(" "))
      .map(r => (r, 1))
      .updateStateByKey[Int](updateFunc)
      .print()
    scc.start()
    scc.awaitTermination()
  }
  val updateFunc = (currentValues: Seq[Int], preValue: Option[Int]) => {
    val curr = currentValues.sum
    val pre = preValue.getOrElse(0)
    Some(curr + pre)
  }

  def createStream(scc: StreamingContext, kafkaParam: Map[String, String], topics: Set[String]) = {
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](scc, kafkaParam, topics)
  }

}

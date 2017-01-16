package com.marstor.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

/**
  * Created by root on 12/16/16.
  * https://www.iteblog.com/archives/1322
  */
object KafkaWordCount {
  def main(args: Array[String]) {
    if(args.length < 4){
      System.err.print("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

//    StreamingExamples.setStreamingLogLevels()

//    val Array(zkQuorum,group,topics,numThreads) = args
//    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
//    val ssc = new StreamingContext(sparkConf,Seconds(2))
//    ssc.checkpoint("/home/checkpoint/kafkaWordCount")
//
//    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
//    val lines = KafkaUtils.createStream(ssc,zkQuorum,group,tipicMap).map(_._2)
//    val words = lines.flatMap(_.split(" "))
//    val wordCounts = words.map(x => (x,1L)).reduceByKeyAndWindow(_ + _, _ - _, Minutes(10) ,Seconds(2),2)
//    wordCounts.print()
//
//    ssc.start()
//    ssc.awaitTermination()

  }

}

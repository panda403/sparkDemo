package com.marstor.training

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 12/20/16.
  * 看过“Lord of the Rings, The (1978)”用户年龄和性别分布
  *
  * data        format
  * ratings.dat UserID::MovieID::Rating::Timestamp
  * movies.dat  MovieID::Title::Genres
  * users.dat   UserID::Gender::Age::Occupation::Zip-code
  */
object MyMovieUserAnalyzer {
  def main(args: Array[String]) {
    val moviePath = "data/movies/ml-1m/movies.dat"
    val ratingPath = "data/movies/ml-1m/ratings.dat"
    val userPath = "data/movies/ml-1m/users.dat"
    val movieName = "Lord of the Rings, The (1978)"

    val conf = new SparkConf().setAppName("MyMovieUserAnalyzer").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val userRdd = sc.textFile(userPath)
    val movieRdd = sc.textFile(moviePath)
    val ratRdd = sc.textFile(ratingPath)

    //
    val movieId = movieRdd.map(_.split("::")).map{ x => (x(0),x(1))}.filter(_._2.equalsIgnoreCase(movieName)).first()._1
//    println(movieId)

    /**
      * UserID,movieid
      */
    val filterRatRdd = ratRdd.map(_.split("::")).map{ x => (x(0),x(1))}.filter(_._2 == movieId)

    /**
      * UserID,((Gender,Age),movieid)
      */
    val joinRdd = userRdd.map(_.split("::")).map{ x => (x(0),(x(1),x(2)))}.join(filterRatRdd)


    val userDistribution = joinRdd.map { x =>
      (x._2._1, 1)
    }.reduceByKey(_ + _)

    userDistribution.foreach(println)

    //  val users = userRdd.
  }
}

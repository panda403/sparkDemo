package com.marstor.training

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 12/20/16.
  * 统计年龄段在“18-24”的男性年轻人，最喜欢看哪10部电影
  *
  * data        format
  * ratings.dat UserID::MovieID::Rating::Timestamp
  * movies.dat  MovieID::Title::Genres
  * users.dat   UserID::Gender::Age::Occupation::Zip-code
  */
object MyTopMovieOfUsers {
  def main(args: Array[String]) {
    val moviePath = "data/movies/ml-1m/movies.dat"
    val ratingPath = "data/movies/ml-1m/ratings.dat"
    val userPath = "data/movies/ml-1m/users.dat"

    val conf = new SparkConf().setAppName("TopMovieOfUsers").setMaster("local[2]")
    val sc = new SparkContext(conf)

    /**
      * UserID,(Gender,Age)
      */
    val userRdd = sc.textFile(userPath).map(_.split("::")).map { x => (x(0), (x(1), x(2))) }
    /**
      * movieId,title
      */
    val movieRdd = sc.textFile(moviePath).map(_.split("::")).map{ x => (x(0),x(1))}
    /**
      * userId,MovieId
      */
    val ratRdd = sc.textFile(ratingPath).map(_.split("::")).map{ x => (x(0),x(1)) }
    /**
      * UserID,(Gender,Age)
      */
    val filterUserRDD = userRdd.filter(_._2._1.equals("M")).filter(_._2._2.equals("18"))
//    filterUserRDD.foreach(println)

    /**
      * MovieId,userId,title
      */
    val ratCombineMoviesRDD = ratRdd.join(movieRdd)
//    ratCombineMoviesRDD.foreach(println)

    /**
      * movieid,((gender,age),(userid,title)))
      */
    val generateRDD = filterUserRDD.join(ratCombineMoviesRDD)
      .map{ x => (x._1,1) }
      .reduceByKey(_ + _)
      .sortByKey(false).take(10)
    generateRDD.foreach(println)

  }

}
package com.marstor.ml

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by root on 2/16/17.
  */
object KMeansClustering {
  def main(args: Array[String]) {

    /**
      * 参数说明：
        trainingDataFilePath:训练数据集文件路径
        testDataFilePath:测试数据集文件路径
        numClusters:聚类的个数
        numIterations:K-means 算法的迭代次数
        runTimes:K-means 算法 run 的次数
      */
    val trainingDataFilePath = "data/ml/Wholesale_customers_data.csv"
    val testDataFilePath = "data/ml/Wholesale_customers_data_training.csv"
    val numClusters = 8
    val numIterations = 30
    val runTimes = 3

//    if (args.length < 5) {
//      println("Usage:KMeansClustering trainingDataFilePath testDataFilePath numClusters numIterations runTimes")
//      sys.exit(1)
//    }

    val conf = new
        SparkConf().setAppName("Spark MLlib Exercise:K-Means Clustering").setMaster("local[3]")
    val sc = new SparkContext(conf)

    /**
      * Channel Region Fresh Milk Grocery Frozen Detergents_Paper Delicassen
      * 2 3 12669 9656 7561 214 2674 1338
      * 2 3 7057 9810 9568 1762 3293 1776
      * 2 3 6353 8808 7684 2405 3516 7844
      */

    val rawTrainingData = sc.textFile(trainingDataFilePath)
    val parsedTrainingData =
      rawTrainingData.filter(!isColumnNameLine(_)).map(line => {
        Vectors.dense(line.split(",").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
      }).cache()

    // Cluster the data into two classes using KMeans

    var clusterIndex: Int = 0
    val clusters: KMeansModel =
      KMeans.train(parsedTrainingData, numClusters, numIterations, runTimes)

    println("Cluster Number:" + clusters.clusterCenters.length)

    println("Cluster Centers Information Overview:")
    clusters.clusterCenters.foreach(
      x => {

        println("Center Point of Cluster " + clusterIndex + ":")

        println(x)
        clusterIndex += 1
      })

    //begin to check which cluster each test data belongs to based on the clustering result

    val rawTestData = sc.textFile(testDataFilePath)
    val parsedTestData = rawTestData.filter(!isColumnNameLine(_)).map(line => {

      Vectors.dense(line.split(",").map(_.trim).filter(!"".equals(_)).map(_.toDouble))

    })
    parsedTestData.collect().foreach(testDataLine => {
      val predictedClusterIndex:
      Int = clusters.predict(testDataLine)

      println("The data " + testDataLine.toString + " belongs to cluster " +
        predictedClusterIndex)
    })

    println("Spark MLlib K-means clustering test finished.")
  }

  private def
  isColumnNameLine(line: String): Boolean = {
    if (line != null &&
      line.contains("Channel")) true
    else false
  }
}

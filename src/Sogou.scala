import org.apache.spark.{SparkConf, SparkContext}

/**
    * Created by Administrator on 2016/6/14.
    */
  object Sogou {
    def main(args: Array[String]) {
      if (args.length == 0) {
        System.err.println("Usage: Sogou <file1> <file2>")
        System.exit(1)
      }

      val conf = new SparkConf().setMaster("local").setAppName("Sogou")
      val sc = new SparkContext(conf)

      //搜索结果排名第1，但是点击次序排在第2的数据有多少?
      val rdd1 = sc.textFile(args(0))
      val rdd2 = rdd1.map(_.split("\t")).filter(_.length == 6)
      //rdd2.count()
      // val rdd3 = rdd2.filter(_ (3).toInt == 1).filter(_ (4).toInt == 2)
      // rdd3.count()
      //rdd3.toDebugString

      //session查询次数排行榜
      val rdd4 = rdd2.map(x => (x(1), 1)).reduceByKey(_ + _).map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1))
      //rdd4.toDebugString
      rdd4.saveAsTextFile(args(1))
      println("ni................................")
      sc.stop()
    }
  }

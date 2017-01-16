package com.marstor.sparksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

//define case class for user
case class User(userID: String, gender: String, age: Int,
                registerDate: String, role: String, region: String)

//define case class for consuming data
case class Order(orderID: String, orderDate: String, productID: Int, price: Int, userID: String)

/**
  * Created by root on 10/20/16.
  */
object UserConsumingDataStatistics {
  def main(args: Array[String]) {

    val userPath = "/panda/spark/data/sample_user_data.txt"
    val consumerPath = "/panda/spark/data/sample_consuming_data.txt"
    val conf = new SparkConf().setAppName("Spark Exercise:User Consuming Data Statistics").setMaster("local[3]")
    //Kryo serializer is more quickly by default java serializer
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    //Convert user data RDD to a DataFrame and register it as a temp table
    val userDF = sc.textFile(userPath).map(_.split(" ")).map(u => User(u(0), u(1), u(2).toInt, u(3), u(4), u(5))).toDF()
    userDF.registerTempTable("user")
    //Convert consuming data RDD to a DataFrame and register it as a temp table
    val orderDF = sc.textFile(consumerPath).map(_.split(" ")).map(o => Order(o(0), o(1), o(2).toInt, o(3).toInt, o(4))).toDF()
    orderDF.registerTempTable("orders")
    //cache the DF in memory with serializer should make the program run much faster
    userDF.persist(StorageLevel.MEMORY_ONLY_SER)
    orderDF.persist(StorageLevel.MEMORY_ONLY_SER)

    //The number of people who have orders in the year 2015
    val count = orderDF.filter(orderDF("orderDate").contains("2015")).join(
      userDF, orderDF("userID").equalTo(userDF("userID"))).count()
    println("The number of people who have orders in the year 2015:" + count)

    //total orders produced in the year 2014
    val countOfOrders2014 = sqlContext.sql("SELECT * FROM orders where  orderDate like ' 2014 % ' ").count()
    println("total orders produced in the year 2014:" + countOfOrders2014)

    //Orders that are produced by user with ID 1 information overview
    val countOfOrdersForUser1 = sqlContext.sql("SELECT o.orderID,o.productID, o.price, u.userID FROM orders o, " +
      "user u where u.userID =  1 and u.userID = o.userID").show()
    println("Orders produced by user with ID 1 showed.")

    //Calculate the max,min,avg prices for the orders that are producted by user with ID 10
    val orderStatsForUser10 = sqlContext.sql("SELECT max(o.price) as maxPrice,  min (o.price) as minPrice, " +
      "avg (o.price) as avgPrice, u.userID FROM orders o,  user u where u.userID = 10 and u.userID = o.userID group by u.userID")
    println("Order statistic result for user with ID 10:")

    orderStatsForUser10.collect().map(order => "Minimum Price=" + order.getAs("minPrice")
      + ";Maximum Price=" + order.getAs("maxPrice")
      + ";Average Price=" + order.getAs("avgPrice")
    ).foreach(result => println(result))
  }
}

package com.marstor.sparksql

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 11/7/16.
  * data like
  * 100, John Smith, Austin, TX, 78727
  * 200, Joe Johnson, Dallas, TX, 75201
  * 300, Bob Jones, Houston, TX, 77028
  * 400, Andy Davis, San Antonio, TX, 78227
  * 500, James Williams, Austin, TX, 78727
  */

// 创建一个表示客户的自定义类
case class Customer(customer_id: Int, name: String, city: String, state: String, zip_code: String)

object SparkSql2Methods {
  def main(args: Array[String]) {
//    createDFByReflect()
    createDFByProgram()
  }
  def createDFByReflect(): Unit = {
    val conf = new SparkConf().setAppName("SparkSql2Methods").setMaster("local")
    val sc = new SparkContext(conf)
    // 首先用已有的Spark Context对象创建SQLContext对象
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // 导入语句，可以隐式地将RDD转化成DataFrame
    import sqlContext.implicits._
    // 用数据集文本文件创建一个Customer对象的DataFrame
    val dfCustomers = sc.textFile("/panda/spark/data/sparksql2methods.txt").map(_.split(",")).map(p => Customer(p(0).trim.toInt, p(1), p(2), p(3), p(4))).toDF()
    // 将DataFrame注册为一个表
    dfCustomers.registerTempTable("customers")
    // 显示DataFrame的内容
    dfCustomers.show()
    // 打印DF模式
    dfCustomers.printSchema()
    // 选择客户名称列
    dfCustomers.select("name").show()
    // 选择客户名称和城市列
    dfCustomers.select("name", "city").show()
    // 根据id选择客户
    dfCustomers.filter(dfCustomers("customer_id").equalTo(500)).show()
    // 根据邮政编码统计客户数量
    dfCustomers.groupBy("zip_code").count().show()
  }

  def createDFByProgram(): Unit = {

    val conf = new SparkConf().setAppName("SparkSql2Methods").setMaster("local")
    val sc = new SparkContext(conf)
    // 用编程的方式指定模式
    // 用已有的Spark Context对象创建SQLContext对象
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // 创建RDD对象
    val rddCustomers = sc.textFile("/panda/spark/data/sparksql2methods.txt")
    // 用字符串编码模式
    val schemaString = "customer_id name city state zip_code"
    // 导入Spark SQL数据类型和Row
    import org.apache.spark.sql._
    import org.apache.spark.sql.types._;
    // 用模式字符串生成模式对象
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    // 将RDD（rddCustomers）记录转化成Row。
    val rowRDD = rddCustomers.map(_.split(",")).map(p => Row(p(0).trim, p(1), p(2), p(3), p(4)))
    // 将模式应用于RDD对象。
    val dfCustomers = sqlContext.createDataFrame(rowRDD, schema)
    // 将DataFrame注册为表
    dfCustomers.registerTempTable("customers")
    // 用sqlContext对象提供的sql方法执行SQL语句。
    val custNames = sqlContext.sql("SELECT name FROM customers")
    // SQL查询的返回结果为DataFrame对象，支持所有通用的RDD操作。
    // 可以按照顺序访问结果行的各个列。
    custNames.map(t => "Name: " + t(0)).collect().foreach(println)
    // 用sqlContext对象提供的sql方法执行SQL语句。
    val customersByCity = sqlContext.sql("SELECT name,zip_code FROM customers ORDER BY zip_code")
    // SQL查询的返回结果为DataFrame对象，支持所有通用的RDD操作。
    // 可以按照顺序访问结果行的各个列。
    customersByCity.map(t => t(0) + "," + t(1)).collect().foreach(println)
  }
}

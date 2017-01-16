//package com.marstor.makedata
//
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.sql.types._
//
///**
//  * Created by root on 10/20/16.
//  */
//object Convert2Parquet {
//
//  def main(args: Array[String]): Unit = {
//
//    def convert(sqlContext: SQLContext, filename: String, schema: StructType, tablename: String) {
//      // import text-based table first into a data frame
//      val df = sqlContext.read.format("com.databricks.spark.csv").
//        schema(schema).option("delimiter", "|").load(filename)
//      // now simply write to a parquet file
//      df.write.parquet("/user/spark/data/parquet/" + tablename)
//    }
//
//    // usage exampe -- a tpc-ds table called catalog_page
//    schema = StructType(Array(
//      StructField("cp_catalog_page_sk", IntegerType, false),
//      StructField("cp_catalog_page_id", StringType, false),
//      StructField("cp_start_date_sk", IntegerType, true),
//      StructField("cp_end_date_sk", IntegerType, true),
//      StructField("cp_department", StringType, true),
//      StructField("cp_catalog_number", LongType, true),
//      StructField("cp_catalog_page_number", LongType, true),
//      StructField("cp_description", StringType, true),
//      StructField("cp_type", StringType, true)))
//    convert(sqlContext,
//      hadoopdsPath + "/catalog_page/*",
//      schema,
//      "catalog_page")
//
//  }
//
//}

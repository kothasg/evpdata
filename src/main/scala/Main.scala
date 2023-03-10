package org.test.evp

import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.tableFunctions.flatten
//import org.apache.spark.sql.functions._


object Main {
  def main(args: Array[String]): Unit = {
    //  need to replace username and password with accesskey and secret key
    val configs = Map (
      //https://uyb88085.us-east-1.snowflakecomputing.com
      //"URL" -> "https://uyb88085.us-east-1.snowflakecomputing.com:443",
      "URL" -> "https://jbdfunl-rfb52751.snowflakecomputing.com",
      "USER" -> "kothasg",
      "PASSWORD" -> "Srini1969",
      "ROLE" -> "ACCOUNTADMIN",
      "WAREHOUSE" -> "COMPUTE_WH",
   //   "DB" -> "SNOWFLAKE_SAMPLE_DATA",
   //   "SCHEMA" -> "TPCH_SF1"

    "DB" -> "TEST",
    "SCHEMA" -> "PUBLIC"
    )
    val session = Session.builder.configs(configs).create
    import session.implicits._
  //  val df = session.read.json("@s3_stage/ElectricVehiclePopulationData.json")
      val df = session.read.json("@s3_stage/test3.json")

    val dfdata1 = df.select(col("$1")("data").as("data"))
    val dfmeta = df.select(col("$1")("meta").as("meta")) //.show(2,150)
     //dfdata1.flatten(col("data")).show(2)
    val datadf = dfdata1.flatten(col("data")).select(col("value").as("data"))//.show(10)
    datadf.show(15,100)
    println(datadf.count())

    val columns1 = dfmeta.select(col("meta")("view")("columns").as("columns1"))
 //   columns1.schema.printTreeString()
   val colnames = columns1.flatten(col(columns1)).select(col("value")("name").as("name"))
    colnames.show(30,150)
    println(colnames.count())
  }
}
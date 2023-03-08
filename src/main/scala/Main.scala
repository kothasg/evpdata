package org.test.evp

import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._

object Main {
  def main(args: Array[String]): Unit = {
    // Replace the <placeholders> below.
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
  //  session.sql("show tables").show()

   // val dfJson = session.read.json("@s3_stage/test.json")

    val dfJson = session.read.json("@s3_stage/test.json").select(col("$1")("data"))
     dfJson.schema.printTreeString();

    dfJson.show(5);
//    val meta = dfJson.select( col("$1"))
//    meta.schema.printTreeString();

  //  val dfjson1 = dfJson.flatten("variant")
  }
}
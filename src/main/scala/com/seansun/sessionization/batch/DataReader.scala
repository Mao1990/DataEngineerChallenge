package com.seansun.sessionization.batch

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

object DataReader {

  val logSchema = StructType(
    Seq(
      StructField("timestamp", TimestampType, nullable = false),
      StructField("elb", StringType, nullable = false),
      StructField("client:port", StringType, nullable = false),
      StructField("backend:port", StringType, nullable = false),
      StructField("request_processing_time", FloatType, nullable = false),
      StructField("backend_processing_time", FloatType, nullable = false),
      StructField("response_processing_time", FloatType, nullable = false),
      StructField("elb_status_code", StringType, nullable = false),
      StructField("backend_status_code", StringType, nullable = false),
      StructField("received_bytes", LongType, nullable = false),
      StructField("sent_bytes", LongType, nullable = false),
      StructField("request", StringType, nullable = false),
      StructField("user_agent", StringType, nullable = false),
      StructField("ssl_cipher", StringType, nullable = false),
      StructField("ssl_protocol", StringType, nullable = false)
    )
  )

  def fromFile(path: String, schema: StructType)(implicit spark: SparkSession): DataFrame = {
    spark.read
      .option("delimiter", " ")
      .schema(schema)
      .csv(path)
  }
}

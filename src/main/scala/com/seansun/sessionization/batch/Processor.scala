package com.seansun.sessionization.batch

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import pureconfig._
import pureconfig.generic.auto._
import com.seansun.sessionization.conf.ApplicationConfig.BatchSessionConfig
import com.seansun.sessionization.core.Sessionizer

object Processor {

  def main(args: Array[String]): Unit = {

    val batchSessionConfig = ConfigSource.default.at("batch").load[BatchSessionConfig]

    batchSessionConfig match {
      case Left(err) =>  println(err.prettyPrint())
      case Right(conf) => {
        implicit val spark: SparkSession = SparkSession.builder().appName("Spark SQL batched Sessionize").getOrCreate()

        val logDf = fromFile(conf.srcPath, logSchema)
          .withColumn("ip", regexp_extract(col("client:port"), "(.+):", 1))
        val sessionDs = Sessionizer.sqlSessionize(logDf, col(conf.userIdField), conf.maxSessionDuration)

        sessionDs.select(avg("sessionLength").as("average session time in second")).show()

        sessionDs
          .groupBy(col("userId"))
          .agg(sum("sessionLength").as("totalSessionTime"))
          .orderBy(col("totalSessionTime").desc)
          .show(10)
        sessionDs.select(
          col("userId"),
          col("sessionId"),
          array_join(col("uniqueRequests"), "|")
        ).write.csv(conf.outputPath)
      }
    }
  }

  val logSchema: StructType = StructType(
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
      StructField("user_agent", StringType, nullable = true),
      StructField("ssl_cipher", StringType, nullable = false),
      StructField("ssl_protocol", StringType, nullable = false)
    )
  )

  def fromFile(path: String, schema: StructType)(implicit spark: SparkSession): DataFrame = {
    spark.read
      .option("delimiter", " ")
      .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXXZ")
      .schema(schema)
      .csv(path)
  }




}

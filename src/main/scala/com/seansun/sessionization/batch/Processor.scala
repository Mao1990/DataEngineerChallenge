package com.seansun.sessionization.batch

import java.io.File
import java.io.FileWriter

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import scopt.OParser
import pureconfig._
import pureconfig.generic.auto._

import com.seansun.sessionization.conf.ApplicationConfig._
import com.seansun.sessionization.core.Sessionizer

object Processor {

  def main(args: Array[String]): Unit = {

    val batchSessionConfig = ConfigSource.default.at("batch").load[BatchSessionConfig]

    batchSessionConfig match {
      case Left(err) => println(err.prettyPrint())
      case Right(conf) => OParser.parse(configParser, args, conf) match {
        case Some(c) => {
          implicit val spark: SparkSession = SparkSession
            .builder()
            .appName("Spark SQL batched Sessionize")
            .getOrCreate()

          val logDf = fromFile(c.srcPath, logSchema)
            .withColumn("ip", regexp_extract(col("client:port"), "(.+):", 1))
          val sessionDs = Sessionizer.sqlSessionize(logDf, col(c.userIdField), c.maxSessionDuration)
              .coalesce(2 * spark.sparkContext.defaultParallelism)
              .cache()

          sessionDs.write.mode(SaveMode.Overwrite).parquet(c.outputPath + "/sessions")
          sessionDs.select(
            col("userId"),
            col("sessionId"),
            array_join(col("uniqueRequests"), "|")
          ).write.mode(SaveMode.Overwrite).csv(c.outputPath + "/sessionUniqueURL")

          val fileWriter = new FileWriter(new File(c.outputPath + "/report.txt"))
          fileWriter.write("The average session length time in second is: ")
          sessionDs
            .select(avg("sessionLength").as("average session time in second"))
            .collect()
            .foreach(line => fileWriter.append(line + "\n\n"))

          fileWriter.append("The following shows the top 10 most engaged user\n\n")

          val top10EngagedDf = sessionDs
            .groupBy(col("userId"))
            .agg(
              sum("sessionLength").as("totalSessionTime"),
              count("sessionId").as("totalSessionCount"),
              sum(size(col("requests"))).as("totalRequestCount"),
              sum(size(col("uniqueRequests"))).as("totalUniqueRequestCount")
            )
            .orderBy(col("totalSessionTime").desc)

          fileWriter.append(top10EngagedDf.columns.mkString("|") + "\n")
          fileWriter.append(top10EngagedDf.columns.map(_ => "---").mkString("|") + "\n")
          top10EngagedDf
            .take(10)
            .foreach(row => fileWriter.append(row.mkString("|") + "\n"))

          fileWriter.close()

        }
        case _ => println("Please fill in the valid options")
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

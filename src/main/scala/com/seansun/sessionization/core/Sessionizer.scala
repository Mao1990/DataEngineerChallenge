package com.seansun.sessionization.core

import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Sessionizer {

  final case class Session(
    userId: String,
    sessionId: String,
    startTimestamp: Long,
    endTimestamp: Long,
    sessionLength: Long,
    count: Long,
    requests: Array[String]
  )
  def sqlSessionize(inputDf: DataFrame, userId: Column, maxSessionDuration: Int)
    (implicit spark: SparkSession): Dataset[Session] = {
    import spark.implicits._
    inputDf
      .withColumn(
        "previousActionTimestamp",
        lag(col("timestamp"), 1).over(Window.partitionBy(userId).orderBy("timestamp")))
      .withColumn(
        "isNewSession",
        when(
          unix_timestamp(col("timestamp")).minus(unix_timestamp(col("previousActionTimestamp")))
            <= lit(maxSessionDuration),
          0
        ).otherwise(1)
      )
      .withColumn(
        "sessionId",
        sum(col("isNewSession"))
          .over(Window.partitionBy(userId).orderBy(userId, col("timestamp"))))
      .groupBy(userId.as("userId"), col("sessionId"))
      .agg(
        min("timestamp").as("startTimestamp"),
        max("timestamp").as("endTimestamp"),
        unix_timestamp(max("timestamp")).minus(unix_timestamp(min("timestamp")))
          .as("SessionLength"),
        count("*").as("count"),
        collect_list(col("request")).as("requests")
      )
      .as[Session]
  }

  val oneHitRequest: Array[String] => Array[String] =
    _.groupBy(r => r).mapValues(_.length).filter(_._2 == 1).keys.toArray

  val oneHitRequestUdf = udf(oneHitRequest)

}

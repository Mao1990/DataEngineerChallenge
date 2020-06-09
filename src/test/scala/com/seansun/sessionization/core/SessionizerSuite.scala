package com.seansun.sessionization.core

import java.time.Instant
import java.sql.Timestamp

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class SessionizerSuite extends FunSuite with DataFrameSuiteBase {

  test("sqlSessionize  should do the good job") {
    val srcSchema: StructType = StructType(
      Seq(
        StructField("id", StringType, nullable = false),
        StructField("timestamp", TimestampType, nullable = false),
        StructField("request", StringType, nullable = false)
      )
    )
    val srcDf = spark.createDataFrame(
      sc.parallelize(
        List(
          Row("id1", java.sql.Timestamp.from(Instant.parse("2015-07-22T09:01:03.850670Z")), "req1"),
          Row("id1", java.sql.Timestamp.from(Instant.parse("2015-07-22T09:01:03.992059Z")), "req1"),
          Row("id1", java.sql.Timestamp.from(Instant.parse("2015-07-22T09:01:04.162111Z")), "req2"),
          Row("id1", java.sql.Timestamp.from(Instant.parse("2015-07-22T09:01:04.336513Z")), "req1"),
          Row("id1", java.sql.Timestamp.from(Instant.parse("2015-07-22T09:01:04.463923Z")), "req1"),
          Row("id1", java.sql.Timestamp.from(Instant.parse("2015-07-22T09:01:04.610330Z")), "req1"),
          Row("id1", java.sql.Timestamp.from(Instant.parse("2015-07-22T09:01:04.782992Z")), "req1"),
          Row("id1", java.sql.Timestamp.from(Instant.parse("2015-07-22T09:01:05.065797Z")), "req2"),
          Row("id1", java.sql.Timestamp.from(Instant.parse("2015-07-22T09:01:05.603659Z")), "req3"),
          Row("id1", java.sql.Timestamp.from(Instant.parse("2015-07-22T09:01:05.897088Z")), "req4"),
          Row("id1", java.sql.Timestamp.from(Instant.parse("2015-07-22T09:01:06.053754Z")), "req1"),
          Row("id1", java.sql.Timestamp.from(Instant.parse("2015-07-22T09:01:09.131348Z")), "req2"),
          Row("id1", java.sql.Timestamp.from(Instant.parse("2015-07-22T09:01:10.559851Z")), "req5"),
          Row("id1", java.sql.Timestamp.from(Instant.parse("2015-07-22T09:01:12.622513Z")), "req6")
        )
      ),
      srcSchema
    )

    val expectSchema = StructType(
      Seq(
        StructField("userId",StringType, nullable = false),
        StructField("sessionId",LongType, nullable = true),
        StructField("startTimestamp",TimestampType, nullable = true),
        StructField("endTimestamp",TimestampType, nullable = true),
        StructField("sessionLength",LongType, nullable = true),
        StructField("count",LongType, nullable = false))
    )
    val expectDf = spark.createDataFrame(
      sc.parallelize(
        List(
          Row(
           "id1",
            1L,
            java.sql.Timestamp.from(Instant.parse("2015-07-22T09:01:03.850670Z")),
            java.sql.Timestamp.from(Instant.parse("2015-07-22T09:01:12.622513Z")),
            9L,
            14L
          )
        )
      ),
      expectSchema
    )
    val actualDf = Sessionizer.sqlSessionize(srcDf, col("id"), 15)(spark)
      .select("userId", "sessionId", "startTimestamp", "endTimestamp", "sessionLength", "count")
    assertDataFrameEquals(expectDf, actualDf)
  }

  test("oneHitRequest should to the good job") {
    val seq = Seq("a", "a", "b", "b", "c")
    val expect = Seq("c")
    val actual = Sessionizer.oneHitRequest.apply(seq)
    assert(expect == actual)
  }
}

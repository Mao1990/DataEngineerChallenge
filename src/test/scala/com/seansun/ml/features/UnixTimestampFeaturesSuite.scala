package com.seansun.ml.features

import java.time.Instant
import java.sql.Timestamp

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class UnixTimestampFeaturesSuite extends FunSuite with DataFrameSuiteBase {

  test("Unix Timestamp Features Extraction") {
    val srcSchema: StringType = StringType(
      Seq(
        StructField("timestamp", TimestampType, nullable = false),
        StructField("requestCount", LongType, nullable = false)
      )
    )

    val srcRdd = sc.parallelize(
      List(
        Row(Timestamp.from(Instant.parse("2015-07-22T09:01:03.850670Z")), 10),
        Row(Timestamp.from(Instant.parse("2015-07-22T09:01:03.850670Z")), 10)
      )
    )
    val srcDf = spark.createDataFrame(srcRdd, srcSchema)

  }


}

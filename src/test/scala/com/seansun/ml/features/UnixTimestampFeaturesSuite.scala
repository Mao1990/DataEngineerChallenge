package com.seansun.ml.features

import java.time.Instant
import java.sql.Timestamp

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, udf}

import org.scalatest.FunSuite


class UnixTimestampFeaturesSuite extends FunSuite with DataFrameSuiteBase {

  test("Unix Timestamp Features Extraction") {

    val vecToSeq = udf((v: Vector) => v.toArray).asNondeterministic
    val srcSchema: StructType = StructType(
      Seq(
        StructField("timestamp", TimestampType, nullable = false),
        StructField("requestCount", LongType, nullable = false)
      )
    )

    val srcRdd = sc.parallelize(
      List(
        Row(Timestamp.from(Instant.parse("2015-07-22T09:01:03.850670Z")), 10L)
      )
    )
    val srcDf = spark.createDataFrame(srcRdd, srcSchema)

    val expectSchema = srcSchema.add(
      StructField("features", VectorType, nullable = false)
    )
    val expectDf = spark.createDataFrame(
      sc.parallelize(
        List(
          Row(
            Timestamp.from(Instant.parse("2015-07-22T09:01:03.850670Z")),
            10L,
            Vectors.dense(30.0,22.0,4.0,203.0,17.0,1.0,3.0)
          )
        )
      ),
      expectSchema
    )

    val unixTimestampFeatures = new UnixTimestampFeatures()
      .setInputCol("timestamp")
      .setOutputCol("features")

    val actualDf = unixTimestampFeatures.transform(srcDf)
    assertDataFrameEquals(expectDf.drop("features"), actualDf.drop("features"))
    assertDataFrameEquals(
      expectDf.select(vecToSeq(col("features"))), actualDf.select(vecToSeq(col("features")))
    )
  }


}

package com.seansun.ml.features

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, dayofmonth, dayofweek, dayofyear, hour, minute, second, weekofyear}
import org.apache.spark.sql.types.{StructField, StructType, TimestampType}

class UnixTimestampFeatures(override val uid: String) extends Transformer with HasInputCol with HasOutputCol {

  def this() = this(Identifiable.randomUID("UnixTimestampFeatures"))

  /** @group setParam */
  def setInputCols(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.isInstanceOf[TimestampType],
      s"The input column must be ${TimestampType.simpleString}, but got ${inputType.catalogString}.")
    val outputColName = $(outputCol)
    if (schema.fieldNames.contains(outputColName)) {
      throw new IllegalArgumentException(s"Output column $outputColName already exists.")
    }
    StructType(schema.fields :+ StructField(outputColName, new VectorUDT, nullable = true))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val assembler = new VectorAssembler()
      .setInputCols(Array("weekofyear", "dayofmonth", "dayofweek", "dayofyear", "hour", "minute", "second"))
      .setOutputCol($(outputCol))
    val initColumns = dataset.columns
    val df = dataset.select(
      col("*"),
      weekofyear(col("unixTimestampMinute")).as("weekofyear"),
      dayofmonth(col("unixTimestampMinute")).as("dayofmonth"),
      dayofweek(col("unixTimestampMinute")).as("dayofweek"),
      dayofyear(col("unixTimestampMinute")).as("dayofyear"),
      hour(col("unixTimestampMinute")).as("hour"),
      minute(col("unixTimestampMinute")).as("minute"),
      second(col("unixTimestampMinute")).as("second")
    )
    assembler.transform(df).select(
      initColumns.map(col) :_*,
      col($(outputCol))
    )
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

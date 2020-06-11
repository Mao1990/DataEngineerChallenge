package com.seansun.ml.loading

import java.io.{File, FileWriter}

import scopt.OParser
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import com.seansun.ml.conf.MachineLearningConfig._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import com.seansun.ml.DataFormat
import com.seansun.ml.features.UnixTimestampFeatures
import com.seansun.sessionization.batch.Processor._
import org.apache.spark.ml.Pipeline



object PredictPipeline {

  final case class PipelineConfig(srcFormat: String, srcPath: String, outputPath: String)
  def main(args: Array[String]): Unit = {
    OParser.parse(linearRegressionConfigParser, args, LinearRegressionConfig(DataFormat.Parquet, "", "")) match {
      case Some(conf) =>
        implicit val spark: SparkSession = SparkSession
          .builder()
          .appName("Spark ML Predict Next Minute Loading")
          .getOrCreate()
        val logDf = conf.dataFormat match {
          case DataFormat.Parquet => fromParquetFile(conf.srcPath)
          case DataFormat.CSV => fromFile(conf.srcPath, logSchema)
        }
        val minuteAggDf = logDf
          .withColumn("unixTimestampMinute", (round(unix_timestamp(col("timestamp"))/60)*60).cast("long"))
          .groupBy(col("unixTimestampMinute")).agg(count(lit(1)).as("requestCount"))


        val unixTimestampFeatures = new UnixTimestampFeatures()
          .setInputCol("unixTimestampMinute")
          .setOutputCol("features")

        val lr = new LinearRegression()
          .setFeaturesCol("features")
          .setLabelCol("requestConut")
          .setMaxIter(50)
          .setRegParam(0.3)
          .setElasticNetParam(0.5)

        val pipeline = new Pipeline()
          .setStages(Array(unixTimestampFeatures, lr))

        val paramGrid = new ParamGridBuilder()
          .addGrid(lr.regParam, Array(0.1, 0.01))
          .build()

        val regressionEvaluator = new RegressionEvaluator()
          .setLabelCol("requestCount")

        val cv = new CrossValidator()
          .setEstimator(pipeline)
          .setEvaluator(regressionEvaluator)
          .setEstimatorParamMaps(paramGrid)
          .setNumFolds(2)  // Use 3+ in practice
          .setParallelism(2)

        val cvModel = cv.fit(minuteAggDf)

        cvModel.write.save(conf.outputPath + "/cvModel")

        val theNextMinuteDf = minuteAggDf
          .select(max(col("unixTimestampMinute")).plus(60).as("unixTimestampMinute"))
        cvModel.transform(theNextMinuteDf)
          .select("unixTimestampMinute", "prediction")
          .collect()
          .foreach { case Row(unixTimestampMinute: Long, prediction: Double) =>
            val wording = s"The next minute: $unixTimestampMinute requests count prediction is $prediction"
            val fileWriter = new FileWriter(new File(conf.outputPath + "/result.txt"))
            fileWriter.write(wording)
            fileWriter.close()
            println(wording)
          }
      case _ => println("Please fill in the valid options")
    }
  }
}

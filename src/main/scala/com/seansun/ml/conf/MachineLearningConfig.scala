package com.seansun.ml.conf

import com.seansun.ml.DataFormat
import scopt.{OParser, OParserBuilder}

object MachineLearningConfig {

  final case class LinearRegressionConfig(
    dataFormat: DataFormat,
    srcPath: String,
    outputPath: String
  )
  val linearRegressionConfigBuilder: OParserBuilder[LinearRegressionConfig] = OParser.builder[LinearRegressionConfig]
  val linearRegressionConfigParser: OParser[Unit, LinearRegressionConfig] = {
    import linearRegressionConfigBuilder._
    OParser.sequence(
      programName("Spark ML LinearRegression Pipeline"),
      head("Spark ML LinearRegression Pipeline"),
      opt[String]("srcPath")
        .required()
        .action((x, c) => c.copy(srcPath = x))
        .text("srcPath is a string property means the source log file path"),
      opt[String]("outputPath")
        .required()
        .action((x, c) => c.copy(outputPath = x))
        .text("outputPath is a string property means the output folder path"),
      opt[String]("dataFormat")
        .optional()
        .action((x, c) => c.copy(dataFormat = DataFormat.withName(x)))
        .text("dataFormat is a string property means the input data format")
    )
  }
}

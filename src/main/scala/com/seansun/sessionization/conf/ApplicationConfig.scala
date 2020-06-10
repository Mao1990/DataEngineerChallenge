package com.seansun.sessionization.conf

import pureconfig._
import pureconfig.generic.auto._
import scopt.{OParser, OParserBuilder}

object ApplicationConfig {

  final case class BatchSessionConfig(
    userIdField: String,
    maxSessionDuration: Int,
    srcPath: String,
    outputPath: String
  )
  val builder: OParserBuilder[BatchSessionConfig] = OParser.builder[BatchSessionConfig]
  val configParser: OParser[Unit, BatchSessionConfig] = {
    import builder._
    OParser.sequence(
      programName("Spark SQL batched Sessionize"),
      head("Spark Batch Sessionize"),
      opt[Int]( "maxSessionDuration")
        .optional()
        .action((x, c) => c.copy(maxSessionDuration = x))
        .text("maxSessionDuration is an integer property"),
      opt[String]("userIdField")
        .optional()
        .action((x, c) => c.copy(userIdField = x))
        .text("userIdField is a string property"),
      opt[String]("srcPath")
        .optional()
        .action((x, c) => c.copy(srcPath = x))
        .text("srcPath is a string property means the source log file path"),
      opt[String]("outputPath")
        .optional()
        .action((x, c) => c.copy(outputPath = x))
        .text("outputPath is a string property means the output folder path")
    )
  }
}

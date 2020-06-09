package com.seansun.sessionization.conf

import pureconfig._
import pureconfig.generic.auto._

object ApplicationConfig {

  final case class BatchSessionConfig(userIdField: String, maxSessionDuration: Int, logPath: String, output: String)

}

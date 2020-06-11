package com.seansun.ml.enums

import enumeratum._

import scala.collection.immutable

sealed trait DataFormat extends EnumEntry

object DataFormat extends Enum[DataFormat] {
  val values: immutable.IndexedSeq[DataFormat] = findValues

  case object CSV extends DataFormat
  case object Parquet extends DataFormat
}

package com.seansun.ml

import scala.collection.immutable

import enumeratum._

sealed trait DataFormat extends EnumEntry

object DataFormat extends Enum[DataFormat] {
  val values: immutable.IndexedSeq[DataFormat] = findValues

  case object CSV extends DataFormat
  case object Parquet extends DataFormat
}

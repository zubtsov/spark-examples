package com.github.zubtsov

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class SparkFunSuite extends AnyFunSuite {
  protected val ss = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "target/spark-warehouse")

  protected val spark = ss
    .getOrCreate()
}

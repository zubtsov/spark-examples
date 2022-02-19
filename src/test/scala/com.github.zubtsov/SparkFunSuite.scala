package com.github.zubtsov

import com.github.zubtsov.SparkFunSuite.SparkWarehousePath
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import scala.reflect.io.Directory


class SparkFunSuite extends AnyFunSuite with BeforeAndAfterAll {
  protected val ss = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.sql.warehouse.dir", SparkWarehousePath)

  protected val spark = ss
    .getOrCreate()

  override protected def beforeAll(): Unit = {
    new Directory(new File(SparkWarehousePath)).deleteRecursively()
  }
}

object SparkFunSuite {
  val SparkWarehousePath = "target/spark-warehouse"
}
package com.github.zubtsov

import com.github.zubtsov.SparkFunSuite.SparkWarehousePath
import org.scalatest.Assertions
import org.apache.spark.sql.functions._

import java.io.File
import scala.reflect.io.Directory

class WriteTests extends SparkFunSuite {
  test("Can't use bucketing with save method") {
    val writer = spark.range(1000)
      .write.format("csv")
      .bucketBy(10, "id")

      Assertions.assertThrows[org.apache.spark.sql.AnalysisException](writer.save("target/bucketed_table1"))
  }

  test("Bucketing controls number of files written in one task (per memory partition)") {
    val numberOfFilesPerTask = 2 //number of tasks = number of partitions in memory
    val numberOfFilePartitions = 5

    val writer = spark.range(0, 1000, 1, numberOfFilePartitions)
      .write.mode("overwrite").format("csv")
      .bucketBy(numberOfFilesPerTask, "id")

    val tableName = "bucketed_table1"
    writer.saveAsTable(tableName)

    val groupedByPartition = new Directory(new File(s"$SparkWarehousePath/$tableName"))
      .list.toSeq
      .filter(p => p.name.endsWith(".csv"))
      .groupBy(p => p.name.substring(0, 10))

    Assertions.assertResult(numberOfFilePartitions)(groupedByPartition.size)
    groupedByPartition.values.foreach(buckets => Assertions.assertResult(numberOfFilesPerTask)(buckets.size))
  }
}

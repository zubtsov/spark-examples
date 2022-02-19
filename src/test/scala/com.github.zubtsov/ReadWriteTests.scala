package com.github.zubtsov

import com.github.zubtsov.SparkFunSuite.SparkWarehousePath
import org.scalatest.Assertions
import org.apache.spark.sql.functions._

import java.io.File
import scala.reflect.io.Directory

class ReadWriteTests extends SparkFunSuite {
  test("Can't use bucketing with save method") {
    val writer = spark.range(1000)
      .write.format("csv")
      .bucketBy(10, "id")

      Assertions.assertThrows[org.apache.spark.sql.AnalysisException](writer.save("target/bucketed_table1"))
  }

  test("Bucketing controls number of files written in each task (per memory partition)") {
    val numberOfFilesPerTask = 2 //number of tasks = number of partitions in memory
    val numberOfFilePartitions = 5

    val df = spark.range(0, 1000, 1, numberOfFilePartitions)

    val writer = df
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

    val readTable = spark.read.table(tableName)
    //oops, bucketing affected the number of partitions, so we got 10 partitions instead of 5...
    Assertions.assertResult(numberOfFilePartitions*numberOfFilesPerTask)(readTable.rdd.getNumPartitions)
  }

  test("Partitioning controls the number of folders and bucketing controls the number of files written in each task (per memory partition)") {
    val numberOfFolderPartitions = 10
    val numberOfFilesPerTask = 2 //number of tasks = number of partitions in memory
    val numberOfFilePartitions = 5

    val writer = spark.range(0, 1000, 1, numberOfFilePartitions)
      .withColumn("partition", col("id") % numberOfFolderPartitions)
      .write.mode("overwrite").format("csv")
      .partitionBy("partition")
      .bucketBy(numberOfFilesPerTask, "id")

    val tableName = "bucketed_table2"
    writer.saveAsTable(tableName)

    val groupedByFolderPartition = new Directory(new File(s"$SparkWarehousePath/$tableName"))
      .deepList(2).toSeq
      .filter(p => p.name.endsWith(".csv"))
      .groupBy(p => p.parent.name)

    Assertions.assertResult(numberOfFolderPartitions)(groupedByFolderPartition.size)
    groupedByFolderPartition.values.foreach(files => {
      val groupedByFilePartition = files.groupBy(p => p.name.substring(0, 10))
      Assertions.assertResult(numberOfFilePartitions)(groupedByFilePartition.size)
      groupedByFilePartition.values.foreach(buckets => Assertions.assertResult(numberOfFilesPerTask)(buckets.size))
    })

    val readTable = spark.read.table(tableName)
    //10 folders * 10 files per folder = .... 15 partitions!
    Assertions.assertResult(15)(readTable.rdd.getNumPartitions)
  }

  test("How to make one file per folder partition") {
    val numberOfFolderPartitions = 10
    val initialNumberOfFilePartitions = 5

    val writer = spark.range(0, 1000, 1, initialNumberOfFilePartitions)
      .withColumn("partition", col("id") % numberOfFolderPartitions)
      .repartition(col("partition")) //key thing here
      .write.mode("overwrite").format("csv")
      .partitionBy("partition")

    val tableName = "bucketed_table3"
    writer.saveAsTable(tableName)

    val groupedByPartition = new Directory(new File(s"$SparkWarehousePath/$tableName"))
      .deepList(2).toSeq
      .filter(p => p.name.endsWith(".csv"))
      .groupBy(p => p.parent.name)

    Assertions.assertResult(numberOfFolderPartitions)(groupedByPartition.size)
    groupedByPartition.values.foreach(files => Assertions.assertResult(1)(files.size))

    val readTable = spark.read.table(tableName)
    //no surprises here
    Assertions.assertResult(numberOfFolderPartitions)(readTable.rdd.getNumPartitions)
  }
}

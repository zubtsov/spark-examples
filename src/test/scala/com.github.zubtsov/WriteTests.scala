package com.github.zubtsov

import org.scalatest.Assertions

class WriteTests extends SparkFunSuite {
  test("Can't use bucketing with save method") {
    val writer = spark.range(1000)
      .write.format("csv")
      .bucketBy(10, "id")

      Assertions.assertThrows[org.apache.spark.sql.AnalysisException](writer.save("target/bucketed_table1"))
  }
}

package com.learn.structured.stream

import org.apache.spark.sql.SparkSession

trait InternalSparkSession {
  val spark: SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

  }
}

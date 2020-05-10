package com.learn.structured.stream.common

import org.apache.spark.sql.SparkSession

trait InternalSparkSession {
  val spark: SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
  }
}

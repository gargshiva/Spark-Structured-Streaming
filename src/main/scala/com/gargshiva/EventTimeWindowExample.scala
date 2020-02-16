package com.gargshiva


import java.sql.Timestamp

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object EventTimeWindowExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error")

    val socketStreamDF: DataFrame = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9001)
      .load()

    import spark.implicits._

    val socketStreamDS: Dataset[String] = socketStreamDF.as[String]

    val stockDS: Dataset[Transaction] = socketStreamDS.map(str => {
      val cols = str.split(" ")
      Transaction(new Timestamp(cols(0).toLong), cols(1), cols(2).toDouble)
    })

    val eventBasedWindowDS = stockDS
      .groupBy(window(col("eventTime"), "100 seconds"))
      .sum("value")

    val query = stockDS
      .writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode(OutputMode.Append())

    query.start().awaitTermination()
  }

}


case class Transaction(eventTime: java.sql.Timestamp, symbol: String, value: Double)
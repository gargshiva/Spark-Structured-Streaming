package com.learn.structured.stream.watermarks

import java.sql.Timestamp

import com.learn.structured.stream.common.InternalSparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object IngestionTimeWindow extends InternalSparkSession {

  def main(args: Array[String]): Unit = {

    import spark.implicits._

    val socketStream = spark
      .readStream
      .format("socket")
      .option("includeTimeStamp", "true")
      .option("host", "localhost")
      .option("port", "50050")

    val socketDS = socketStream
      .load()
      .as[(String, Timestamp)]

    /**
      * Introduced delay of 15 seconds..
      */
    val stockDF = socketDS
      .map(event => {
        Thread.sleep(15000)
        val values = event._1.split(",")
        StockData(new Timestamp(values(0).toLong), event._2, values(1), values(2).toDouble)
      })

    /**
      * Window of 10 seconds on Ingestion Time
      */
    val windowDF = stockDF
      .withWatermark("ingestionTime", "500 milliseconds")
      .groupBy(
        col("symbol"),
        window(col("ingestionTime"), "10 seconds")
      ).max("value")


    windowDF
      .writeStream
      .format("console")
      .option("checkpointLocation", "/Users/shigarg1/hadoop/stocks/checkpoint")
      .outputMode(OutputMode.Complete())
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }

}

case class StockData(eventTime: Timestamp, ingestionTime: Timestamp, symbol: String, value: Double)
package com.learn.structured.stream.watermarks

import java.sql.Timestamp

import com.learn.structured.stream.common.InternalSparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

object WatermarksRoller extends InternalSparkSession {

  def main(args: Array[String]): Unit = {

    import spark.implicits._

    val stockSchema = StructType(Array(
      StructField("time", TimestampType),
      StructField("symbol", StringType),
      StructField("value", DoubleType)
    ))

    val fileStream = spark
      .readStream
      .option("maxFilesPerTrigger", 1)
      .schema(stockSchema)
      .csv("src/main/resources/stocks")
      .as[Stock]

    val windowedMax = fileStream
      .withWatermark("time", "500 milliseconds")
      .groupBy(
        col("symbol"),
        window(col("time"), "10 seconds")
      ).max("value")

    windowedMax
      .writeStream
      .format("console")
      .option("checkpointLocation", "/Users/shigarg1/hadoop/stocks/checkpoint")
      .outputMode(OutputMode.Update())
      .option("truncate", "false")
      .start()
      .awaitTermination()

  }
}

case class Stock(time: Timestamp, symbol: String, value: Double)

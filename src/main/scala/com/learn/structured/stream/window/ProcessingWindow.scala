package com.learn.structured.stream.window

import java.sql.Timestamp

import com.learn.structured.stream.common.InternalSparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

/**
  * In Spark Streaming ,
  * Event time : At source ; Time at which event happened; Say IOT device
  * Ingestion time : Time at which event ingested to the System ; Say IOT devices published to Kafka/Redis Stream;
  */

/**
  * Processing time :
  * Time tracked by Processing engine when data was  arrived for processing ;
  * Time lapsed is signified by central clock maintained at Driver;
  * Last 10 seconds of processing time => Collect all the records which arrived at processing engine in last 10 seconds
  */

/**
  * Windowed Word count on Processing time
  */
object ProcessingWindow extends InternalSparkSession {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    //DataSource API
    val socketDF = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "50050")
      .load()

    /**
      * To apply window on processing time, need a column of timestamp
      */
    val timedDS = socketDF
      .withColumn("processingTime", current_timestamp())
      .as[(String, Timestamp)]

    val wordDS: DataFrame =
      timedDS
        .flatMap(pair => pair._1.split(" ").map(ele => (ele, pair._2)))
        .toDF("word", "processingTime")

    /**
      * Collect events of last 15 seconds of processing time
      * Apply aggregation on them
      */

    val windowedCount = wordDS
      .groupBy(
        col("word"),
        window(col("processingTime"), "15 seconds")
      )
      .count()
      .orderBy("window")

    //DataSink
    val dataStreamWriter = windowedCount
      .writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode(OutputMode.Complete())

    // Streaming query
    dataStreamWriter.start().awaitTermination()


  }
}

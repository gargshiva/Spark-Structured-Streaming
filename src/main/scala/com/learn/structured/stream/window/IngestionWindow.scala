package com.learn.structured.stream.window

import java.sql.Timestamp

import com.learn.structured.stream.common.InternalSparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode


/**
  * Ingestion timestamp :
  * Time at which particular event ingested in the system;
  * Example : IOT events (eventTime) ingested to kafka (ingestion time) and processed by Spark Streaming (processing time);
  */

object IngestionWindow extends InternalSparkSession {

  def main(args: Array[String]): Unit = {

    import spark.implicits._

    /**
      * DataSource API
      * SocketStream is the ingestion medium ; It includes timestamp at which particular event ingested.
      * Similar to kafka
      */

    val socketDF = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "50050")
      .option("includeTimestamp", "true")
      .load()

    val socketDS = socketDF.as[(String, Timestamp)]

    /**
      * Processing takes longer ; Sleep 10 seconds
      */
    val wordDF = socketDS
      .flatMap(pair => {
        Thread.sleep(10000)
        pair._1.split(" ").map(ele => (ele, pair._2))
      })
      .toDF("word", "timestamp")

    /**
      * 15 seconds of Window on Ingestion timestamp;
      * Collect all the events in last 15 seconds of ingestion timestamp
      * Apply aggregation on all the collected events;
      *
      */
    val wordCountDF = wordDF
      .groupBy(
        window(col("timestamp"), "15 seconds")
      ).count().orderBy(col("window"))

    //Sink + Streaming query
    wordCountDF
      .writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .option("truncate", "false")
      .start()
      .awaitTermination()


  }
}

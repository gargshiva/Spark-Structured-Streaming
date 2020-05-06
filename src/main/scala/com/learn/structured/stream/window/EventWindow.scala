package com.learn.structured.stream.window

import java.sql.Timestamp

import com.learn.structured.stream.common.InternalSparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

/**
  * Event Time :
  * Time at which event is generated at source;
  * 2 things :
  *     - When event occurred ? -> about where a given event fits in event time line
  *     - How long ago event occurred ? -> tracking the passing of time;
  */

/**
  * Whenever we say , what is the max value of a particular stock in last 10 seconds of event time ?
  *   - How Spark knows all the records for 10 seconds event time window have reached into processing system ? - Passage of time
  *   - By default spark wait for late events -  infinite delay
  *   - All the windows are remembered as state to handle late events ; Say for window (10 - 20 seconds) , event can comes after days..      (Problem 1)
  *   - As time goes number of windows increases and they use more and more resources.
  *
  * By default, spark remembers all the windows forever and waits for the late events forever.               s
  *
  * How long spark should wait (problem 2)
  */

/**
  * Solution -> Watermarking solution to prob1 and prob2
  *
  * Watermarks is a threshold , which defines the how long Spark wait for the late events?
  * We can limit the state, number of windows to be remembered, using custom watermarks.?
  *
  * Spark will only allow the events to be processed which is newer than :
  * Max(event timestamp , processing engine has seen ) - Threshold
  */
object EventWindow extends InternalSparkSession {

  def main(args: Array[String]): Unit = {
    import spark.implicits._

    //DataSourceAPI
    val socketStream =
      spark
        .readStream
        .format("socket")
        .option("host", "localhost")
        .option("port", "50050")
        .load()
        .as[String]

    val stockDS: Dataset[Stock] = socketStream
      .map(value => {
        val columns = value.split(",")
        Stock(new Timestamp(columns(0).toLong), columns(1), columns(2).toDouble)
      })

    /**
      * 10 seconds window on the event time
      */
    val windowedSum = stockDS
      .groupBy(
        window(col("time"), "10 seconds")
      ).sum("value").orderBy("window")

    /**
      * Data Sink + Streaming query
      */
    windowedSum
      .writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }

}


case class Stock(time: Timestamp, symbol: String, value: Double)
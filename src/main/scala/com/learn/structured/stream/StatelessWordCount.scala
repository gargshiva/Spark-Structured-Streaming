package com.learn.structured.stream

import org.apache.spark.sql.streaming.{DataStreamReader, OutputMode}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * By default;
  * Spark Streaming track the state from very beginning / across stream / across micro-batches;
  * Cost : State management + state recovery
  */

/**
  * Stateless aggregations
  *   - You want to have aggregations within microbatch;
  *   - You want to have aggregations for the records from last 5 seconds;
  */

/**
  * Structured Streaming doesn't have the concept of batch time ;
  * Frequency of Processing is specified using Trigger API.
  * By default , Trigger.ProcessingTime(0) - ASAP ,  analogous to the batch time of DStream API
  */

/**
  * Count the words for every 5 seconds
  * Aggregation is done on the data which is collected for last 5 seconds.
  * The state is only kept for those 5 seconds and the forgotten.
  * So in case of failure, we need to recover data only for last 5 seconds
  */
object StatelessWordCount {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = {
      SparkSession.builder()
        .master("local[*]")
        .getOrCreate()

    }

    import spark.implicits._

    // DataSource
    val dataStreamReader: DataStreamReader = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 50050)

    val socketDS = dataStreamReader.load().as[String]

    // Operations - Use stateless API
    // Aggregation within micro-batch
    val wordCountDF: DataFrame = socketDS
      .flatMap(line => line.split(" "))
      .groupByKey(v => v)
      .flatMapGroups {
        case (value, itr) => Iterator((value, itr.length))
      }.toDF("value", "count")

    // DataSink - Define micro-batch over here using Trigger
    // By default , Trigger is Trigger.ProcessingTime(0)
    val streamingQuery = wordCountDF
      .writeStream
      .format("console")
      .outputMode(OutputMode.Update())
      .start()

    streamingQuery.awaitTermination()

    // DataSink - Define micro-batch over here using Trigger(define processing frequency)
    // Trigger - 5 seconds
    /* val streamingQuery_TimedBatch = wordCountDF
       .writeStream
       .format("console")
       .outputMode(OutputMode.Update())
       .trigger(Trigger.ProcessingTime(FiniteDuration(5, TimeUnit.SECONDS)))
       .start()

     streamingQuery_TimedBatch.awaitTermination()*/
  }

}

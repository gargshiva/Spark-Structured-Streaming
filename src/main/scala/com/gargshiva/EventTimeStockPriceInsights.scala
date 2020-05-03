package com.gargshiva

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Calculate the average of a listed stock .
  *
  * Spark waits for certain interval (0 seconds / asap) , collect data from source and create MicroBatch.
  * MaxRecordsPerTrigger: 50 K => One micro batch will consist maximum of 50 K records.
  *
  * Driver (DAG scheduler + Task Scheduler) schedules the micro batch in form of tasks to be run on executors.
  * As micro batch completes ; Spark will again fetch data from Source and create another Microbatch.
  *
  * Trigger : if specified , means how long spark need to wait before checking if new data is available.
  * If no trigger is specified ,then It fetch data from source after first batch finished.
  *
  * The Spark Streaming engine stores the state of aggregates (in this case the last avg value) after each query in memory or on disk when checkpointing is enabled.
  *
  * This allows it to merge the value of aggregate functions computed on the partial (new) data with the value of the same aggregate functions computed on previous (old) data.
  *
  * After the new states are computed, they are checkpointed (if enabled)
  *
  * Checkpointing : Offsets,Commits,Source,State(Stores the aggregation),Metadata
  *
  * Window : Tumbling Window based on eventTime . Infinite window for late events.
  *
  * Structured Streaming API implicitly maintains the state across batches for aggregate functions FOREVER.
  *
  */
object EventTimeStockPriceInsights {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // DataSource API to read the data from various structured resources ; Data is represented with DataFrame API.
   val op: DataFrame =  spark.read.format("").load("")

    import spark.implicits._
    val socketDataSet = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9000)
      .load()
      .as[String]

    val stockDataSet: Dataset[Stock] = socketDataSet.map(Stock(_))


    val finalDS = stockDataSet
      .withWatermark("timestamp", "2 seconds")

      .groupBy(window(col("timestamp"), "10 seconds"), col("id"))
      .avg("price").alias("Avg Price")


    finalDS.explain(true)

    /**
      * No Trigger specified : Process the next batch as first micro batch finish
      */
    val streamQuery = finalDS
      .writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode(OutputMode.Append())

    streamQuery.start().awaitTermination()
  }

}




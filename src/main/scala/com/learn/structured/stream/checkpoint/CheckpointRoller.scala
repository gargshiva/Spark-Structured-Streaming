package com.learn.structured.stream.checkpoint

import com.learn.structured.stream.common.InternalSparkSession
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter, OutputMode}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}

/**
  * By default, Spark streaming maintains the state from very beginning;
  * In case of failures/restarts; Spark need to recover the state to avoid re-run.
  * Checkpoint is the mechanism to preserve the state of the applications in local folder/HDFS.
  */

/**
  * To enable checkpiunt , source of data must support it ;
  * Checkpoint may need to replay some of the data to fully recover the state ; Like re-playing the messages in kafka from particular offset;
  * SocketStream doesn't support checkpoints;
  * Kafka and FileStream support checkpointing;
  */

/**
  * Recovery :
  *
  * Whenever Spark streaming query restarts; spark goes through the content of the directory before it accepts any new data.
  * This makes sure that spark recovers the old state before it starts processing new data.
  * So whenever there is restart, spark first recovers the old state and then start processing new data from the stream.
  */
object CheckpointRoller extends InternalSparkSession {

  def main(args: Array[String]): Unit = {

    import spark.implicits._

    /**
      * No schema inference in the Spark Streaming
      * Mandatory to specify the schema for the source connector
      */
    val salesSchema = StructType(Array(
      StructField("transactionId", StringType),
      StructField("customerId", StringType),
      StructField("itemId", StringType),
      StructField("amountPaid", DoubleType))
    )

    /**
      * DataSource API to read the csv files from folder
      * maxFilesPerTrigger indicates the number of file used in 1 micro-batch (sort of size base micro-batch)
      */

    val dataStream: DataStreamReader = spark
      .readStream
      .option("header", "true")
      .option("maxFilesPerTrigger", 1)
      .schema(salesSchema)

    val salesDS: Dataset[Sales] = dataStream.csv("src/main/resources/sales").as[Sales]

    val customerDS: Dataset[Customer] =
      spark.read
        .option("header", "true")
        .csv("src/main/resources/customer.csv")
        .as[Customer]
        .cache()

    //Operations
    val joinedDS = customerDS.join(salesDS, "customerId")

    val countDs = joinedDS.groupBy("customerId").sum("amountPaid")

    //DataSink
    val sink: DataStreamWriter[Row] = countDs
      .writeStream
      .format("console")
      .option("checkpointLocation", "src/main/resources/checkpoint")
      .outputMode(OutputMode.Complete())
      .queryName("CheckpointQuery")


    //Streaming Query
    val streamingQuery = sink.start()
    streamingQuery.awaitTermination()
  }

}


case class Customer(
                     customerId: String,
                     customerName: String)

case class Sales(
                  transactionId: String,
                  customerId: String,
                  itemId: String,
                  amountPaid: Double)
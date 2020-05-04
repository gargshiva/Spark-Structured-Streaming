package com.learn.structured.stream.checkpoint

import com.learn.structured.stream.common.InternalSparkSession
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter, OutputMode}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}

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
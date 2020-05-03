package com.learn.structured.stream

import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter, OutputMode, StreamingQuery}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * DataSourceAPI (1.3) has been extended to support stream with new method readStream() and writestream()
  * DataSourceAPI to read the data from various structured resources.
  * Data is represented as DataFrame API.
  */

/**
  * In streams , Schema Inference is not supported by DataSourceAPI as opposite to the batch API
  * Schema of the data has to be given by the DataSource Connector (KakfaConnector, RedisConnector, SocketConnector)
  * SocketConnector -  The schema contains single column named value of the type string
  */

/**
  * Create Source
  * Create Sink (Connect source and Sink)
  * Create Streaming Query - Query executing continuously as new data arrives.
  */

/**
  * Where we have specified batch time (Spark 1.x) ? Or what is the frequency of the processing the data ?
  * SS don't have batch time ; It  specify the processing frequency using the Trigger abstraction;
  * By default Trigger is ProcessingTime(0) that means process the data asap. Similar to  per message semantics of the other streaming systems like storm.
  *
  */

object SourceAndSinkRoller {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()


    // DataSource
    val dataStreamReader: DataStreamReader = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 50050)

    val streamDF = dataStreamReader.load()

    // DataSink
    val dataStreamWriter: DataStreamWriter[Row] = streamDF
      .writeStream
      .format("console")
      .outputMode(OutputMode.Append())

    // Streaming query
    val streamingQuery: StreamingQuery =
      dataStreamWriter
        .queryName("ConsoleSinkQuery")
        .start()

    streamingQuery.awaitTermination()

  }
}

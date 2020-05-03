package com.learn.structured.stream

import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter, OutputMode, StreamingQuery}

/**
  * Output mode :
  *   Output of the Spark Streaming is the dataset (Infinite table);
  *   How the data is written to infinite table depends on the Output mode;
  *   Output mode 3 types : Append , Complete , Update
  *     Append : Records in the ongoing batch will be written to Sink ; It has no effect on the already processed batches (No aggregations supported)
  *     Compete : For ongoing batch , Entire output table is re-written to sink. It gives global result. Aggregations Supported
  *     Update: For ongoing batch; Only the rows which are changed (new + effected the already processed one) will be written.
  */


/**
  * State management ;
  *   - By default , Every aggregation is stateful and gives global result;
  *   - That means , Spark keep track of the state across the micro-batches/stream ; (as opposite to the DStreams)
  *   Spark remembers the state from beginning ; No matter what how many days,months,years;
  *
  */

object StatefulWordCount extends InternalSparkSession {

  def main(args: Array[String]): Unit = {

    import spark.implicits._

    // DataSource
    val dataStreamReader: DataStreamReader = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 50050)

    val socketDS = dataStreamReader.load().as[String]

    // Operations
    val wordCountDS = socketDS
      .flatMap(line => line.split(" "))
      .groupBy("value")
      .count()

    // DataSink
    val dataSink: DataStreamWriter[Row] = wordCountDS
      .writeStream
      .queryName("WordCountRoller")
      .format("console")
      .outputMode(OutputMode.Update())


    //Streaming query
    val streamingQuery: StreamingQuery = dataSink.start()

    streamingQuery.awaitTermination()
  }

}

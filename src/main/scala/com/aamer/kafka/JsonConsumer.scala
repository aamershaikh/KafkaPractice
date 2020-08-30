package com.aamer.kafka

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions.{col, from_json}

// consume JSON from kafka topic (initial load)

object JsonConsumer {
  def main(args: Array[String]): Unit = {

    // initialize spark session
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkJsonConsoleReader")
      .getOrCreate()

    // disable logging
    spark.sparkContext.setLogLevel("ERROR")

    // initialize bootstrap server and subscribe to initial load topic
    val intialdataframe = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe","newTopic1")
      .option("startingOffsets", "earliest")
      .load()

    intialdataframe.printSchema()

    // schema against which the incoming message from kafka will be validated
    // this is used for converting JSON to dataframe
    val schema = new StructType()
      .add("id",IntegerType)
      .add("firstname",StringType)
      .add("lastname",StringType)


    // extract the "value" which is JSON string( "from_json" ...) and
    // convert it into dataframe using the custom schema defined above
    val personStringInitialDataframe = intialdataframe
                                .selectExpr("CAST(value AS STRING)")
                                .select(from_json(col("value"), schema).as("data"))
                                .select("data.*")

    personStringInitialDataframe.createTempView("initialDataload")

    // use option path and checkpoint location only if we have to write it to a file system
    val query = personStringInitialDataframe.writeStream
      .format("console")
      //.option("path", "/Users/aamershaikh/Documents/ToDelete_SparkLogs")
      //.option("checkpointLocation", "/Users/aamershaikh/Documents/ToDelete_SparkLogs")
      .outputMode("append")
      .start()
      .awaitTermination()
   }
}

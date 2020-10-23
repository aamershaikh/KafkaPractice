package com.aamer.kafka

import org.apache.spark.sql.{SaveMode, SparkSession}

object AvroToJsonConvertor extends App {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("avrotojsonconvertor")
      .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


  //read avro file
    val df = spark.read.format("avro")
      .load("/Users/aamershaikh/Documents/ScalaSparkKafkaProducerConsumer/resources/userdata1.avro")

  df.show()

  df.printSchema()

  df.write.mode(SaveMode.Overwrite).json("/Users/aamershaikh/Documents/ScalaSparkKafkaProducerConsumer/temp/userdata1.json")


}

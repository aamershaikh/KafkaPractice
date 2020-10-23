package com.aamer.kafka

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.avro.from_avro

object AvroToJsonConvertorWithAvscInferSchema extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("avrotojsonconvertor")
    .getOrCreate()

  val jsonFormatSchema = new String(
    Files.readAllBytes(Paths.get("/Users/aamershaikh/Documents/ScalaSparkKafkaProducerConsumer/resources/userdata.avsc"))
  )

  val userDf = spark.read.format("avro").
    load("/Users/aamershaikh/Documents/ScalaSparkKafkaProducerConsumer/resources/userdata1.avro")

  val output = userDf.select(from_avro('value, jsonFormatSchema) as 'output)

  output.write.mode(SaveMode.Overwrite).json("/Users/aamershaikh/Documents/ScalaSparkKafkaProducerConsumer/temp/userdata2.json")

}

package com.aamer.kafka

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}


object JsonConsumerWithStaticValues extends App {
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("JsonConsumerWithStaticValues")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  // static load of simple data from local storage
  val simpleData = Seq(
    (1,"GHI","LMP"),
    (2,"ABC","NYC")
  )
  val df = simpleData.toDF("id","firstname","lastname")
  df.printSchema()
  df.createTempView("initialLoad")
  df.show()

  // read simpleData2 from kafka
  val intialdataframe = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe","newTopic1")
    .option("startingOffsets", "earliest")
    .load()

  // schema against which the incoming message from kafka will be validated
  // this is used for converting JSON to dataframe
  val schema = new StructType()
    .add("id",IntegerType)
    .add("firstname",StringType)
    .add("lastname",StringType)


  // extract the "value" which is JSON string( "from_json" ...) and
  // convert it into dataframe using the custom schema defined above
  val personStringDeltaDataframe = intialdataframe
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).as("data"))
    .select("data.*")
  personStringDeltaDataframe.printSchema()
  personStringDeltaDataframe.createTempView("deltaLoad")

  val finalMasterDf = df.union(personStringDeltaDataframe)
  finalMasterDf.printSchema()
  finalMasterDf.createTempView("masterData")
  finalMasterDf.show()

  // use option path and checkpoint location only if we have to write it to a file system
  val query = finalMasterDf.writeStream
    .format("console")
    //.option("path", "/Users/aamershaikh/Documents/ToDelete_SparkLogs")
    //.option("checkpointLocation", "/Users/aamershaikh/Documents/ToDelete_SparkLogs")
    .outputMode("append")
    .start()
    .awaitTermination()
}

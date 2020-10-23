package com.aamer.kafka

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object JsonConsumerWithBothStaticValues extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("JsonConsumerWithStaticValues")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  // load of simple data from local storage
  val initialData = Seq(
    (1,"Aamer","Shaikh"),
    (4,"ABC","DEF"),
    (5, "VUP","MAN")
  )

  var initialdf = initialData.toDF("id", "firstname", "lastname")
  initialdf.printSchema()
  initialdf.createOrReplaceGlobalTempView("a")
  initialdf.show()

  // read simpleData2 from kafka
  val deltaData = Seq(
    (1,"Aamer","KLM"),
    (2,"NOP","XYZ")
  )

  val deltadf2 = deltaData.toDF("id","firstname","lastname")
  deltadf2.printSchema()
  deltadf2.createOrReplaceGlobalTempView("b")
  deltadf2.show()



  initialdf = deltadf2.union(initialdf).dropDuplicates("id")
  initialdf.printSchema()
  initialdf.createTempView("masterData")

  initialdf.show()

  // use option path and checkpoint location only if we have to write it to a file system
  /*val query = finalMasterDf.writeStream
    .format("console")
    .outputMode("append")
    .start()
    .awaitTermination()*/

}

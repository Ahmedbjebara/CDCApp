package org.datarox

import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object WriteReadDFFromKafkaTopic {

  def WriteReadDF(wholeFileDF : DataFrame)(implicit spark1 : SparkSession) =
  {

  val schema = new StructType()
    .add("ACTION", StringType)
    .add("ID", IntegerType)
    .add("PRENOM", StringType)
    .add("DATE", StringType)




  import org.apache.spark.sql.functions.to_json
  import spark1.implicits._
  import org.apache.spark.sql.functions.struct

//  val dfToJson = wholeFileDF.select(to_json(struct($"ACTION", $"ID", $"PRENOM", $"DATE"))as "data")
//
//
//
//  dfToJson.selectExpr(" data  as value")
//      .write
//    .format("kafka")
//    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
//    .option("topic", "nidha43")
//    .save()



  val df1 = spark1
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("subscribe", "nidha43")
    .load()
    .selectExpr("CAST(value AS STRING)")
      .as[(String)]




    val query2 = df1.writeStream
        .format("csv")
      .option("path","C:\\Users\\dell\\Desktop\\aa")
    .option("checkpointLocation", "/target")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query2.awaitTermination()


 import  org.apache.spark.sql.functions._

  df1.select(from_json(col("value").cast("string"), schema).as("data"))
   .select("data.*")


  }
}

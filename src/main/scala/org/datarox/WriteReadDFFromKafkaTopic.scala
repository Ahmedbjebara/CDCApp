package org.datarox

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

  val dfToJson = wholeFileDF.select(to_json(struct($"ACTION", $"ID", $"PRENOM", $"DATE"))as "data")



  dfToJson.selectExpr(" data  as value")
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("topic", "nidha11")
    .save()




  val df1 = spark1
    .read
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("subscribe", "nidha11")
    .option("enable.auto.commit","true")
    .option("auto.offset.reset","latest")
  // .option("fromOffset", "latest")
    .load()



  import  org.apache.spark.sql.functions._

   df1.select(from_json(col("value").cast("string"), schema).as("data"))
    .select("data.*")


  }
}

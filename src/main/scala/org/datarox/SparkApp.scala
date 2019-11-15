package org.datarox

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datarox.CDCServices._

import scala.io.Source

object SparkApp {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils")

    implicit val spark1 = SparkSession.builder
      .appName("SparkSessionExample")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate


    if ( args.length < 1 ) {
      System.err.println("Argument number's is not respected")
      System.exit(1)
    }


    val argFile = args(0)
    val arrayString = Source.fromFile(argFile).mkString.split("\n")
    val filePath = arrayString(0).trim

    val cdcDF = initiateHiveTable()
    val wholeFileDF = readDFFromFile(filePath)


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
      .option("topic", "nidhal2")
      .save()


    val df1 = spark1
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("subscribe", "nidhal2")
      .option("startingOffsets","earliest")
      .load()



    df1.printSchema()
    df1.show(true)

    import  org.apache.spark.sql.functions._

    val df2 = df1.select(from_json(col("value").cast("string"), schema).as("data"))
      .select("data.*")

    df2.printSchema()
    df2.show(true)



    val insertActionDF = df2.where(df2("ACTION") === 'I').drop("ACTION")
    insertActionDF.show()
    val updateActionDF = df2.where(df2("ACTION") === 'U').drop("ACTION")
    updateActionDF.show()
    val deleteActionDF = df2.where(df2("ACTION") === 'D').drop("ACTION")
    deleteActionDF.show()


val dfInsert = insert(insertActionDF, cdcDF)
Thread.sleep(100)
val dfUpdate = update(updateActionDF, dfInsert)
Thread.sleep(100)
val dfDelete = delete(deleteActionDF, dfUpdate)

dfDelete.show()


    Thread.sleep(100000)


  }

}

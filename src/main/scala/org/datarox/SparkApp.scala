package org.datarox

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.datarox.OperationsOnHiveTable._
import org.datarox.WriteReadDFFromKafkaTopic._

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
    val joinKey = arrayString(1).trim



    val tableDF = initiateHiveTable()

    val wholeFileDF = spark1.read.format("csv")
      .option("sep", ";")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(filePath)

    val df2 = WriteReadDF(wholeFileDF  )


    val insertActionDF = df2.where(df2("ACTION") === 'I').drop("ACTION")
    insertActionDF.show()
    val updateActionDF = df2.where(df2("ACTION") === 'U').drop("ACTION")
    updateActionDF.show()
    val deleteActionDF = df2.where(df2("ACTION") === 'D').drop("ACTION")
    deleteActionDF.show()


val dfInsert = insert(insertActionDF, tableDF)
Thread.sleep(100)
val dfUpdate = update(updateActionDF, dfInsert , joinKey)
Thread.sleep(100)
val dfDelete = delete(deleteActionDF, dfUpdate,joinKey )



    dfDelete.write.mode("append").insertInto("CDC")
    spark1.sql("SELECT * FROM CDC").show()



    Thread.sleep(10000)


  }

}

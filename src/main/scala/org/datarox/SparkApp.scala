package org.datarox

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datarox.CDCServices._
import scala.io.Source

object SparkApp {
  def main(args: Array[String]): Unit = {


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


    val insertActionDF = wholeFileDF.where(wholeFileDF("ACTION") === 'I').drop("ACTION")
    val updateActionDF = wholeFileDF.where(wholeFileDF("ACTION") === 'U').drop("ACTION")
    val deleteActionDF = wholeFileDF.where(wholeFileDF("ACTION") === 'D').drop("ACTION")


    val dfInsert = insert(insertActionDF, cdcDF)
    Thread.sleep(100)
    val dfUpdate = update(updateActionDF, dfInsert)
    Thread.sleep(100)
    val dfDelete = delete(deleteActionDF, dfUpdate)

    dfDelete.show()


    Thread.sleep(100000)


  }

}

package org.datarox

import org.apache.spark.sql.SparkSession
import org.datarox.extendApp.{Insertfunct, initiateHiveTable, readfunct}

import scala.io.Source

object SparkAppli {
  def main(args: Array[String]): Unit = {


    implicit val spark =  SparkSession.builder
      .master("local[*]")
      .appName("Application")
      .enableHiveSupport()
      .getOrCreate()

    if (args.length < 1) {
      System.err.println("Argument number's is not respected")
      System.exit(1)
    }

    val argfile= args(0)
    val  arrayString = Source.fromFile(argfile).mkString.split("\n")

    val filePath =arrayString(0).trim

    initiateHiveTable()
   val fileToDF = readfunct(filePath )
    Insertfunct(fileToDF)

  }
}

package org.datarox

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datarox.extendApp._
import scala.io.Source

object SparkAppli {
  def main(args: Array[String]): Unit = {



   implicit  val spark1 = SparkSession.builder
      .appName("SparkSessionExample")
      .master("local[*]")
     .enableHiveSupport()
      .getOrCreate

    import spark1.implicits

    if (args.length < 1) {
      System.err.println("Argument number's is not respected")
      System.exit(1)
    }


    //val argFile  = args(0)
   // val  arrayString = Source.fromFile(argFile).mkString.split("\n")

    val filePath ="C:/Users/dell/Desktop/aa/changements.csv"
    //arrayString(0).trim
   // spark1.sql("DROP TABLE CDC")
   val  cdcDF= initiateHiveTable()
   val fileToDF = readfunct(filePath)
    val Action =readActionFromFile(filePath).collect.map(x=>x.getString(0))


     Action.map (x => x match {
    case "I" =>{ Insertfunct(fileToDF)
      println("Insert")}
    case "D"=> {deletefunct(fileToDF , cdcDF)
      println("delete")}
    case "U" =>{ updatefunct(fileToDF , cdcDF)
      println("update")}
  }
  )
    cdcDF.show()
    spark1.sql(" SELECT * FROM CDC").show()
  }

}

package org.datarox

import org.apache.spark.sql.{DataFrame, SparkSession}

object extendApp  {

  def initiateHiveTable()(implicit spark : SparkSession):DataFrame ={
    //sparkk.sql("DROP TABLE CDC")
      spark.sql(" CREATE TABLE IF NOT EXISTS CDC(  ID STRING , PRENOM STRING , DATE STRING)  row format delimited fields terminated by ';'")

    }


  def   readfunct(filePath : String)(implicit  spark :  SparkSession):DataFrame ={

    spark.read.format("csv")
      .option("sep", ";")
      .option("header", "true")
        .option("inferSchema", "true")
      .load(filePath)
      .select("Id", "Prenom", "Date")
  }

  def  Insertfunct(fileToDF :DataFrame)(implicit  spark :  SparkSession):DataFrame={
    fileToDF.write.mode("append").insertInto("CDC")
     spark.sql(" SELECT * FROM CDC")
  }

}

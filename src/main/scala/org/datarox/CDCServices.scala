package org.datarox

import org.apache.spark.sql.{DataFrame, SparkSession}

object CDCServices {

  def initiateHiveTable()(implicit spark1: SparkSession): DataFrame = {
    //spark1.sql("DROP TABLE CDC")
    spark1.sql(" CREATE TABLE IF NOT EXISTS CDC(  ID STRING , PRENOM STRING , DATE STRING)  row format delimited fields terminated by ';'")
    spark1.sql("SELECT * FROM CDC")
  }


  def readDFFromFile(filePath: String)(implicit spark1: SparkSession): DataFrame = {
    spark1.read.format("csv")
      .option("sep", ";")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(filePath)
  }

  def insert(fileDF: DataFrame, cdcDF: DataFrame): DataFrame = {
    fileDF.union(cdcDF)
  }


  def delete(fileToDF: DataFrame, cdcDF: DataFrame): DataFrame = {
    cdcDF.except(fileToDF)
  }


  def update(fileToDF: DataFrame, cdcDF: DataFrame): DataFrame = {
    val tempDF = cdcDF.join(fileToDF, "ID").select(cdcDF("ID"), cdcDF("PRENOM"), cdcDF("DATE"))
    val DFU = cdcDF.except(tempDF)
    DFU.union(fileToDF)
  }
}

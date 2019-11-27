package org.datarox

import org.apache.spark.sql.{DataFrame, SparkSession}

object OperationsOnHiveTable {

  def initiateHiveTable()(implicit spark1: SparkSession): DataFrame = {
  spark1.sql("DROP TABLE CDC")
    spark1.sql(" CREATE TABLE IF NOT EXISTS CDC(  ID STRING , PRENOM STRING , DATE STRING)  row format delimited fields terminated by ';'")
    spark1.sql("SELECT * FROM CDC")
  }



  def insert(fileDF: DataFrame, tableDF: DataFrame): DataFrame = {
    fileDF.union(tableDF)
  }


  def delete(fileToDF: DataFrame, tableDF: DataFrame,joinKey : String): DataFrame = {
    val tempDF = tableDF.join(fileToDF, joinKey).select(tableDF("ID"), tableDF("PRENOM"), tableDF("DATE"))
    tableDF.except(tempDF)
  }


  def update(fileToDF: DataFrame, tableDF: DataFrame, joinKey : String): DataFrame = {
    val tempDF = tableDF.join(fileToDF, joinKey).select(tableDF("ID"), tableDF("PRENOM"), tableDF("DATE"))
    val DFU = tableDF.except(tempDF)
    DFU.union(fileToDF)
  }
}

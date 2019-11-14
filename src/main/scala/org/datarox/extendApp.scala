package org.datarox

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datarox.SparkAppli
object extendApp  {

  def initiateHiveTable()(implicit spark1 : SparkSession):DataFrame ={
   // spark1.sql("DROP TABLE CDC")
      spark1.sql(" CREATE TABLE IF NOT EXISTS CDC(  ID STRING , PRENOM STRING , DATE STRING)  row format delimited fields terminated by ';'")
   spark1.sql("SELECT * FROM CDC")

    }

  def   readfunct(filePath : String)(implicit  spark1 :  SparkSession):DataFrame ={

    spark1.read.format("csv")
      .option("sep", ";")
      .option("header", "true")
        .option("inferSchema", "true")
      .load(filePath)
      .select("ID", "PRENOM", "DATE")
  }


  def readActionFromFile(filePath: String)(implicit  spark1 :  SparkSession)={

  spark1.read.format("csv")
    .option("sep", ";")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(filePath)
    .select("ACTION")
  }

  def  Insertfunct(fileToDF :DataFrame)(implicit  spark1 :  SparkSession):DataFrame={
    fileToDF.write.mode("overwrite").insertInto("CDC")
     spark1.sql(" SELECT * FROM CDC")
  }


  def  deletefunct(fileToDF :DataFrame, cdcDF:DataFrame):DataFrame={
val joinedDeletedDF=    fileToDF.join(cdcDF,"ID").select(fileToDF("ID"),fileToDF("PRENOM"),fileToDF("DATE"))
 val finalDeleteDF =   cdcDF.except(joinedDeletedDF)
    finalDeleteDF.write.mode("append").insertInto("CDC")
    finalDeleteDF
  }



  def  updatefunct(fileToDF :DataFrame, cdcDF:DataFrame)(implicit  spark1 :  SparkSession):DataFrame={

    val newPrenom=    fileToDF.join(cdcDF,"ID").select(fileToDF("PRENOM")).first().getString(0)

    val sameId=    fileToDF.join(cdcDF,"ID").select(fileToDF("ID")).first().getInt(0)

    val newDate=    fileToDF.join(cdcDF,"ID").select(fileToDF("DATE")).first().getString(0)

    import spark1.implicits._
    val tempDF = Seq ((sameId,newPrenom,newDate)).toDF("ID", "PRENOM", "DATE")

    val joinedDeletedDF=    cdcDF.join(fileToDF,"ID").select(cdcDF("ID"),cdcDF("PRENOM"),cdcDF("DATE"))
    joinedDeletedDF.show()
    val finalDeleteDF =   cdcDF.except(joinedDeletedDF)
    finalDeleteDF.write.mode("overwrite").insertInto("CDC")

       tempDF.write.mode("append").insertInto("CDC")
    val finalUpdatedDF = spark1.sql(" SELECT * FROM CDC")
      finalUpdatedDF
  }


}

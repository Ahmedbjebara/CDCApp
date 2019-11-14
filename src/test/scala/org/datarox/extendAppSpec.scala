package org.datarox

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datarox.extendApp._
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import org.datarox.SharedSparkSession

case class User(Id: Int, Prenom: String, Date: String)

case class customers(ID: Int, PRENOM: String, DATE: String)

class extendAppSpec extends FlatSpec with Matchers with GivenWhenThen with SharedSparkSession {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("Application")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()


  behavior of "extendAppSpec"

  import spark.implicits._



  val simpleDF: DataFrame = Seq(User(1, "JACER", "13/11/2019"),
    User(2, "AHMED", "14/11/2019"),
    User(3, "NIDHAL", "15/11/2019"))
    .toDF("ID", "PRENOM", "DATE")

  it should "read records from csv file into dataframe" in {
    Given("an absolute path of a csv file")
    val filePath = "C:\\Users\\dell\\Desktop\\aa\\changements.csv"
    When("readfunct is invoked")
    val resultatDF = readfunct(filePath)
    Then(" dataframe should be returned ")
    resultatDF.collect() should contain theSameElementsAs simpleDF.collect()

  }


  spark.sql("DROP TABLE CDCFINAL")

  spark.sql(" CREATE TABLE IF NOT EXISTS CDCFINAL(  ID INT , PRENOM STRING , DATE STRING)  row format delimited fields terminated by ';'")

  spark.sql(" INSERT INTO CDCFINAL  VALUES (1, 'JACER', '13/11/2019')")
  spark.sql(" INSERT INTO CDCFINAL  VALUES (2, 'AHMED', '14/11/2019')")
  spark.sql(" INSERT INTO CDCFINAL  VALUES (3, 'NIDHAL', '15/11/2019')")
  //spark.sql(" INSERT INTO CDCFINAL  VALUES (1, 'JACER', '13/11/2019')")
  val dfTest = spark.sql("SELECT * FROM CDCFINAL")


  spark.sql("DROP TABLE CDC")
  spark.sql(" CREATE TABLE IF NOT EXISTS CDC(  ID INT , PRENOM STRING , DATE STRING)  row format delimited fields terminated by ';'")


  it should "insert records from dataframe into dataframe" in {

    Given("a dataframe ")

    import spark.implicits._

    val fileToDF = Seq(customers(1, "JACER", "13/11/2019"),
      customers(2, "AHMED", "14/11/2019"),
      customers(3, "NIDHAL", "15/11/2019"))
      .toDF("ID", "PRENOM", "DATE")

    // val fileToDF =  Seq(customers(1, "JACER", "13/11/2019")).toDF("ID", "PRENOM", "DATE")

    When("Insertfunct is invoked")
    val finalDF = Insertfunct(fileToDF)
    // finalDF.show()


    Then(" hive table should be returned ")
    finalDF.collect() should contain theSameElementsAs dfTest.collect()

  }



  val refDF = Seq(User(4, "MED", "14/11/2019"), User(5, "ZEINEB", "15/11/2019") ).toDF("ID", "PRENOM", "DATE")

  it should "delete records from dataframe" in {
    Given("two dataframes ")
    val firstFileDF: DataFrame = Seq(User(1, "JACER", "13/11/2019"),
      User(2, "AHMED", "14/11/2019"),
      User(3, "NIDHAL", "15/11/2019"))
      .toDF("ID", "PRENOM", "DATE")
    spark.sql("DROP TABLE cdcTest")
    spark.sql(" CREATE TABLE IF NOT EXISTS cdcTest(  ID INT , PRENOM STRING , DATE STRING)  row format delimited fields terminated by ';'")
    spark.sql(" INSERT INTO cdcTest  VALUES (1, 'JACER', '13/11/2019')")
    spark.sql(" INSERT INTO cdcTest  VALUES (4, 'MED', '14/11/2019')")
    spark.sql(" INSERT INTO cdcTest  VALUES (5, 'ZEINEB', '15/11/2019')")
    val secondCdcDF = spark.sql("SELECT * FROM cdcTest")

    When("deletefunct is invoked")
    val resultatDFrame = deletefunct(firstFileDF, secondCdcDF)



    Then(" dataframe should be returned ")
    resultatDFrame.collect() should contain theSameElementsAs refDF.collect()

  }

  val refDataFrame = Seq(User(5, "Mehdi", "17/11/2019"), User(4, "Mariem", "15/11/2019") ).toDF("ID", "PRENOM", "DATE")

  it should "update records from dataframe" in {
    Given("two dataframes ")
    val firstFileToDF: DataFrame = Seq(User(1, "JACER", "13/11/2019"),
      User(2, "AHMED", "14/11/2019"),
      User(3, "NIDHAL", "15/11/2019"),
        User (4, "Mariem", "15/11/2019"))
      .toDF("ID", "PRENOM", "DATE")
    spark.sql("DROP TABLE cdcTestUpdate")
    spark.sql(" CREATE TABLE IF NOT EXISTS cdcTestUpdate(  ID INT , PRENOM STRING , DATE STRING)  row format delimited fields terminated by ';'")
    spark.sql(" INSERT INTO cdcTestUpdate  VALUES (5, 'Mehdi', '17/11/2019')")
    spark.sql(" INSERT INTO cdcTestUpdate  VALUES (4, 'Med', '14/11/2019')")

    val updatedCdcDF = spark.sql("SELECT * FROM cdcTestUpdate")

    firstFileToDF.show()
    updatedCdcDF.show()

    When("deletefunct is invoked")
    val resultatUpdatedDFrame = updatefunct(firstFileToDF, updatedCdcDF)
    resultatUpdatedDFrame.show()
    firstFileToDF.show()
    updatedCdcDF.show()
    resultatUpdatedDFrame.show()

    Then(" dataframe should be returned ")
    resultatUpdatedDFrame.collect() should contain theSameElementsAs refDataFrame.collect()

  }
  spark.sql(" SELECT * FROM CDC")

}

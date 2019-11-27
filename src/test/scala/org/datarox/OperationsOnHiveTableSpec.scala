package org.datarox

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datarox.OperationsOnHiveTable._
import org.scalatest.{FlatSpec,FunSuite, GivenWhenThen, Matchers}


case class User(Id: Int, Prenom: String, Date: String)

case class Person(Action: String, Id: Int, Prenom: String, Date: String)

case class customers(ID: Int, PRENOM: String, DATE: String)

class OperationsOnHiveTableSpec extends FlatSpec with Matchers with GivenWhenThen {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("Application")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()


  behavior of "OperationsOnHiveTableSpec"

  import spark.implicits._
  val joinKey = "ID"

  val dfTest = Seq(customers(1, "JACER", "13/11/2019"),
    customers(2, "AHMED", "14/11/2019"),
    customers(3, "NIDHAL", "15/11/2019"),
    customers(4, "MEHDI", "16/11/2019"),
    customers(5, "ACHREF", "17/11/2019"),
    customers(6, "MED", "18/11/2019"))
    .toDF("ID", "PRENOM", "DATE")


  it should "insert records from dataframe into dataframe" in {

    Given("two dataframes , one from external file and the other from HIVE Table ")

    import spark.implicits._

    val fileToDF = Seq(customers(1, "JACER", "13/11/2019"),
      customers(2, "AHMED", "14/11/2019"),
      customers(3, "NIDHAL", "15/11/2019"))
      .toDF("ID", "PRENOM", "DATE")

    val cdcDF = Seq(customers(4, "MEHDI", "16/11/2019"),
      customers(5, "ACHREF", "17/11/2019"),
      customers(6, "MED", "18/11/2019"))
      .toDF("ID", "PRENOM", "DATE")

    // val fileToDF =  Seq(customers(1, "JACER", "13/11/2019")).toDF("ID", "PRENOM", "DATE")

    When("Insertfunct is invoked")
    val finalDF = insert(fileToDF, cdcDF)
    // finalDF.show()


    Then("Union Dataframe  should be returned ")
    finalDF.collect() should contain theSameElementsAs dfTest.collect()
    finalDF.show()
  }


  val refDF = Seq(customers(3, "NIDHAL", "15/11/2019"),
    customers(4, "MEHDI", "16/11/2019"),
    customers(5, "ACHREF", "17/11/2019"),
    customers(6, "MED", "18/11/2019"))
    .toDF("ID", "PRENOM", "DATE")

  it should "delete records from dataframe" in {
    Given("two dataframes ")
    val firstFileDF: DataFrame = Seq(User(1, "JACER", "13/11/2019"),
      User(2, "AHMED", "14/11/2019"))
      .toDF("ID", "PRENOM", "DATE")

    val secondCdcDF = Seq(customers(1, "JACER", "13/11/2019"),
      customers(2, "AHMED", "14/11/2019"),
      customers(3, "NIDHAL", "15/11/2019"),
      customers(4, "MEHDI", "16/11/2019"),
      customers(5, "ACHREF", "17/11/2019"),
      customers(6, "MED", "18/11/2019"))
      .toDF("ID", "PRENOM", "DATE")

    When("deletefunct is invoked")
    val resultatDFrame = delete(firstFileDF, secondCdcDF,joinKey )


    Then(" dataframe should be returned ")
    resultatDFrame.collect() should contain theSameElementsAs refDF.collect()

  }

  val refDataFrame = Seq(User(1, "MARIEM", "11/11/2019"),
    User(2, "AMINE", "14/11/2019"),
    User(3, "MALEK", "16/11/2019"),
    User(4, "Mariem", "15/11/2019"))
    .toDF("ID", "PRENOM", "DATE")


  it should "update records from dataframe" in {
    Given("two dataframes ")
    val firstFileToDF: DataFrame = Seq(User(1, "MARIEM", "11/11/2019"),
      User(2, "AMINE", "14/11/2019"),
      User(3, "MALEK", "16/11/2019"))
      .toDF("ID", "PRENOM", "DATE")

    firstFileToDF.show()

    val updatedCdcDF = Seq(User(1, "JACER", "13/11/2019"),
      User(2, "AHMED", "14/11/2019"),
      User(3, "NIDHAL", "15/11/2019"),
      User(4, "Mariem", "15/11/2019"))
      .toDF("ID", "PRENOM", "DATE")

    updatedCdcDF.show()

    When("updatefunct is invoked")
    val resultatUpdatedDFrame = update(firstFileToDF, updatedCdcDF,joinKey )
    resultatUpdatedDFrame.show()

    Then(" dataframe should be returned ")
    resultatUpdatedDFrame.collect() should contain theSameElementsAs refDataFrame.collect()

  }
}

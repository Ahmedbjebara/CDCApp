package org.datarox
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datarox.extendApp._
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers}
import org.datarox.SharedSparkSession

case class User(Id: Int, Prenom: String, Date: String)
case class customers(ID: Int , PRENOM: String, DATE: String)

class extendAppSpec extends FlatSpec with Matchers with GivenWhenThen with SharedSparkSession {

implicit val spark : SparkSession = SparkSession
  .builder()
  .appName("Application")
  .master("local[*]")
  .enableHiveSupport()
  .getOrCreate()


  behavior of "extendAppSpec"

  import spark.implicits._

  val simpleDF:DataFrame = Seq(User(1, "JACER", "13/11/2019"),
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


  it should "insert records from dataframe into hive table" in {

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

  spark.sql(" SELECT * FROM CDC").show()
}

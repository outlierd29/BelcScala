package spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Example02 {
  def main(args: Array[String]): Unit = {
    val csvPath = "/home/davicin/IdeaProjects/belcorp/src/data/soccer/"
    val conf = new SparkConf()
      .setAppName("CapacitacionSpark")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      // .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    /* Archivo csv*/

    val dfCountry = spark.read.format(source = "csv").option("header",true).load(path = csvPath + "Country.csv")
    dfCountry.show(numRows = 5)
    dfCountry.printSchema()

    val dfMatch = spark.read.format(source = "csv").option("header",true).load(path = csvPath + "Match.csv")
    dfMatch.show()
    dfMatch.createOrReplaceTempView(viewName = "Match")

    val dfResult = spark.sql(sqlText = "SELECT count(1) FROM Match WHERE league_id = 2")
    dfResult.show()
  }

}

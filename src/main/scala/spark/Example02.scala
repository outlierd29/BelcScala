package spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Example02 {
  def main(args: Array[String]): Unit = {
    val csvCountryPath = "/home/davicin/IdeaProjects/belcorp/src/data/soccer/Country.csv"
    val conf = new SparkConf()
      .setAppName("CapacitacionSpark")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      // .enableHiveSupport()
      .getOrCreate()

    /* Archivo csv*/

    val dfCountry = spark.read.format(source = "csv").option("header",true).load(path = csvCountryPath)
    dfCountry.show(numRows = 5)
  }

}

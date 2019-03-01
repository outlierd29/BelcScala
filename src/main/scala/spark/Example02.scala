package spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Example02 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("CapacitacionSpark")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      // .enableHiveSupport()
      .getOrCreate()

    /* Archivo csv*/

    val dfCsv =
  }

}

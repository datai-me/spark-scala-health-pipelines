// =============================================================
// PROJET : EPIDEMIC BIG DATA PIPELINE
// STACK  : Scala 2.13 | Spark 3.x | Delta Lake
// CAS    : Surveillance épidémiologique à partir d'une API publique
// =============================================================

package pipeline

import org.apache.spark.sql.SparkSession
import config.ConfigLoader

object Main {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
    // System.setProperty("spark.hadoop.io.native.lib.available", "false")

    val spark = SparkSession.builder()
      .appName(ConfigLoader.appName)
      .master(ConfigLoader.master)
      .getOrCreate()

    EpidemicPipelineApp.run(spark)

    spark.stop()
  }
}

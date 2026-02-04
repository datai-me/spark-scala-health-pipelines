// =============================================================
// PROJET : EPIDEMIC BIG DATA PIPELINE
// STACK  : Scala 2.13 | Spark 3.x | Delta Lake
// CAS    : Surveillance épidémiologique à partir d'une API publique
// =============================================================

package pipeline

import org.apache.spark.sql.SparkSession
import config.ConfigLoader
import org.apache.logging.log4j.LogManager

object Main {

  private val logger = LogManager.getLogger(Main.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("Démarrage Spark Session")
    val spark = SparkSession.builder()
      .appName(ConfigLoader.appName)
      .master(ConfigLoader.master)
      .getOrCreate()

    // Affichage des versions Java/Scala/Spark/Hadoop
    println(s"Java: ${System.getProperty("java.version")}")
    println(s"Scala: ${util.Properties.versionString}")
    println(s"Spark: ${spark.version}")
    println(s"Hadoop: ${org.apache.hadoop.util.VersionInfo.getVersion}")

    // Lancer le pipeline
    EpidemicPipelineApp.run(spark)

    spark.stop()
  }
}

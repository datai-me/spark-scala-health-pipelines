// =============================================================
// PROJET : EPIDEMIC BIG DATA PIPELINE
// STACK  : Scala 2.13 | Spark 3.x | Delta Lake
// CAS    : Surveillance épidémiologique à partir d'une API publique
// =============================================================

package pipeline

import org.apache.spark.sql.SparkSession
import config.ConfigLoader
import utils.VersionsInfo
import pipeline.EpidemicPipelineApp
import org.apache.logging.log4j.LogManager

object Main {

 private val logger = LogManager.getLogger(Main.getClass)

  def main(args: Array[String]): Unit = {

    logger.info("Starting Epidemic Big Data Pipeline")

    // 🔹 Création UNIQUE du SparkSession
    val spark = SparkSession.builder()
      .appName("Epidemic Big Data Pipeline")
      .master("local[*]") // utilise tous les cœurs disponibles, à retirer en production
      //.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      //.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")	  
      .getOrCreate()

    // 🔹 Affichage des versions AU DÉMARRAGE
    VersionsInfo.printVersions(spark)

    // 🔹 Exécution du pipeline
    EpidemicPipelineApp.run(spark)

    // 🔹 Arrêt propre
    spark.stop()
    logger.info("Pipeline stopped successfully")
  }
}

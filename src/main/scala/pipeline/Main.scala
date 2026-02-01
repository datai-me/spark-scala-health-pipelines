// =============================================================
// PROJET : EPIDEMIC BIG DATA PIPELINE
// STACK  : Scala 2.13 | Spark 3.x | Delta Lake
// CAS    : Surveillance épidémiologique à partir d'une API publique
// =============================================================

package pipeline

import org.apache.spark.sql.SparkSession
import utils.VersionsInfo
import pipeline.EpidemicPipelineApp

object Main {

  def main(args: Array[String]): Unit = {

    // 🔹 Création UNIQUE du SparkSession
    val spark = SparkSession.builder()
      .appName("Epidemic Big Data Pipeline")
      .master("local[*]") // à retirer en production
      .getOrCreate()

    // 🔹 Affichage des versions AU DÉMARRAGE
    VersionsInfo.printVersions(spark)

    // 🔹 Exécution du pipeline
    EpidemicPipelineApp.run(spark)

    // 🔹 Arrêt propre
    spark.stop()
  }
}

// -------------------------------------------------------------
// FICHIER : pipeline/EpidemicPipelineApp.scala
// RÔLE    : Point d'entrée du pipeline
// -------------------------------------------------------------

package pipeline

import config.SparkSessionBuilder
import ingestion.EpidemicApiIngestion
import quality.DataQualityRules
import transformation.SilverTransformations
import analytics.GoldAggregations
import storage.DeltaWriter

/**
 * Pipeline principal épidémiologique.
 * Contient uniquement la logique métier.
 * N'est PAS le point d'entrée JVM.
 */
object EpidemicPipelineApp {

  /**
   * Méthode principale du pipeline
   * Appelée depuis Main.scala
   */
  def run(): Unit = {

    // 1. Initialisation Spark
    val spark = SparkSessionBuilder.build("spark-scala-health-pipelines")

    try {
      // 2. INGESTION – BRONZE
      val bronzeDF = EpidemicApiIngestion.ingest(spark)

      // 3. DATA QUALITY – SILVER
      val silverClean = DataQualityRules.applyRules(bronzeDF)
      val silverDF = SilverTransformations.normalize(silverClean)

      // 4. ANALYTIQUE – GOLD
      val goldDF = GoldAggregations.indicators(silverDF)

      // 5. PERSISTENCE
      DeltaWriter.write(bronzeDF, "data/bronze/epidemic")
      DeltaWriter.write(silverDF, "data/silver/epidemic")
      DeltaWriter.write(goldDF, "data/gold/epidemic")

    } finally {
      // 6. Arrêt propre de Spark
      spark.stop()
    }
  }
}
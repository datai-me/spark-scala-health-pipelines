// -------------------------------------------------------------
// FICHIER : pipeline/EpidemicPipelineApp.scala
// RÔLE    : Point d'entrée du pipeline
// -------------------------------------------------------------

package pipeline

import org.apache.spark.sql.{DataFrame, SparkSession}
import config.ConfigLoader
import org.apache.logging.log4j.LogManager

object EpidemicPipelineApp {

  private val logger = LogManager.getLogger(EpidemicPipelineApp.getClass)

  def run(spark: SparkSession): Unit = {

    logger.info("Epidemic pipeline started")

    val df = loadEpidemicData(spark)
    df.show(false)

    logger.info("Epidemic pipeline finished")
  }

  private def loadEpidemicData(spark: SparkSession): DataFrame = {

    logger.info(s"Reading data from API: ${ConfigLoader.epidemicApiUrl}")

    spark.read
      .format("json")
      .option("multiline", "true")
      .load(ConfigLoader.epidemicApiUrl)
  }
}

// -------------------------------------------------------------
// FICHIER : pipeline/EpidemicPipelineApp.scala
// RÔLE    : Point d'entrée du pipeline
// -------------------------------------------------------------

package pipeline

import org.apache.spark.sql.{DataFrame, SparkSession}

object EpidemicPipelineApp {

  /**
   * Point d’entrée du pipeline métier
   * SparkSession est injecté depuis Main
   */
  def run(spark: SparkSession): Unit = {

    println(">>> Epidemic Pipeline started")

    val epidemicDF = loadSampleData(spark)
    epidemicDF.show(false)

    println(">>> Epidemic Pipeline finished")
  }

  /**
   * Exemple de chargement de données
   */
  private def loadSampleData(spark: SparkSession): DataFrame = {
    import spark.implicits._

    Seq(
      ("Madagascar", "COVID-19", 120000, "2024-12-31"),
      ("Kenya", "Cholera", 3400, "2024-11-15")
    ).toDF("country", "disease", "cases", "report_date")
  }
}

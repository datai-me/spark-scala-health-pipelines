package sql

import model.EpidemicCountry
import org.apache.spark.sql.Dataset

/**
 * Exécution de requêtes Spark SQL
 * + affichage dans la console
 */
object EpidemicSqlRunner {

  def run(ds: Dataset[EpidemicCountry]): Unit = {

    // Création d’une vue SQL temporaire
    ds.createOrReplaceTempView("epidemic")

    val spark = ds.sparkSession

    // Requête SQL analytique
    val result = spark.sql(
      """
        |SELECT
        |  country,
        |  cases,
        |  deaths,
        |  population,
        |  ROUND(cases * 100.0 / population, 2) AS infection_rate
        |FROM epidemic
        |WHERE population > 10000000
        |ORDER BY cases DESC
        |""".stripMargin
    )

    // Affichage console
    result.show(20, truncate = false)
  }
}

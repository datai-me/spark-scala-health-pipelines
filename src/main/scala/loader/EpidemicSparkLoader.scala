package loader

import api.EpidemicApiClient
import config.SparkSessionProvider
import model.EpidemicCountry
import org.apache.spark.sql.Dataset

/**
 * Chargement des données API dans Spark
 */
object EpidemicSparkLoader {

  def load(): Dataset[EpidemicCountry] = {

    val spark = SparkSessionProvider.spark
    import spark.implicits._

    // 1. Appel API (driver only)
    val rawJson = EpidemicApiClient.fetchRawJson()

    // 2. Parsing JSON → objets Scala
    val data = EpidemicApiClient.parse(rawJson)

    // 3. Conversion vers Dataset Spark
    spark.createDataset(data)
  }
}

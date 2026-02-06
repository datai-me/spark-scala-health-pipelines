package ingestion

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import config.ConfigLoader

import service.EpidemicApiServiceAsync

import scala.concurrent.Await
import scala.concurrent.duration._

object EpidemicApiIngestion {

  /**
   * Lecture API REST épidémique via service async (Akka HTTP),
   * puis parsing Spark SQL.
   *
   * Note: readApi reste synchrone (DataFrame) -> on attend le Future.
   */
  def readApi(spark: SparkSession): DataFrame = {

    // 1️⃣ Appel HTTP ASYNC -> on attend le résultat (timeout de sécurité)
    // Ajuste la durée si tu veux (ex: via env)
    val rawJson: String =
      Await.result(EpidemicApiServiceAsync.fetchEpidemicJson(), 60.seconds)

    // 2️⃣ Conversion String → Dataset[String] (Encoder via implicits)
    import spark.implicits._
    val jsonDS = Seq(rawJson).toDS()

    // 3️⃣ Lecture JSON par Spark
    val rawDf = spark.read
      .option("multiLine", true)
      .option("inferSchema", true)
      .json(jsonDS)

    // 4️⃣ Flatten / sélection colonnes
    rawDf.select(
      col("updated"),
      col("country"),
      col("countryInfo.iso2").as("iso2"),
      col("countryInfo.iso3").as("iso3"),
      col("continent"),
      col("population"),
      col("cases"),
      col("deaths"),
      col("recovered"),
      col("active"),
      col("critical"),
      col("tests"),
      col("casesPerOneMillion"),
      col("deathsPerOneMillion")
    )
  }
}

package ingestion

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import scala.io.Source
import java.net.URL
import config.ConfigLoader
import service.EpidemicApiService

object EpidemicApiIngestion {

  /**
   * Lecture API REST épidémique
   */
  def readApi(spark: SparkSession): DataFrame = {
	
	// 1️⃣ Appel HTTP
    val rawJson: String =
      Source.fromURL(new URL(ConfigLoader.epidemicApiUrl)).mkString
	
	val draftJson = EpidemicApiService.fetchEpidemicJson()

    // 2️⃣ Conversion String → Dataset[String]
    import spark.implicits._
    val jsonDS = Seq(rawJson).toDS()

    // 3️⃣ Lecture JSON par Spark
    val rawDf = spark.read
      .option("multiLine", true)
	  .option("inferSchema", true)
      .json(jsonDS)	

    // Explosion du countryInfo + flatten
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
      col("critical")
    )
  }
}

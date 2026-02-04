// -------------------------------------------------------------
// FICHIER : pipeline/EpidemicPipelineApp.scala
// RÔLE    : Point d'entrée du pipeline
// -------------------------------------------------------------

package pipeline

import org.apache.spark.sql.{DataFrame, SparkSession}
import config.ConfigLoader
import org.apache.logging.log4j.LogManager
import scala.io.Source
import java.net.{URL, HttpURLConnection}
import java.nio.file.{Files, Paths}
import java.io.IOException

object EpidemicPipelineApp {

  private val logger = LogManager.getLogger(EpidemicPipelineApp.getClass)

  // Fonction principale
  def run(spark: SparkSession): Unit = {
    logger.info("Pipeline démarré")
    val df = loadEpidemicData(spark)
    df.printSchema()
    df.show(10, truncate = false)
    writeToDB(df)
    logger.info("Pipeline terminé")
  }

  // Charger les données depuis l'API avec retry et timeout
  private def loadEpidemicData(spark: SparkSession): DataFrame = {
    var attempt = 0
    var lastEx: Option[Throwable] = None
    while (attempt < ConfigLoader.apiRetries) {
      try {
        logger.info(s"Tentative ${attempt + 1} de lecture API ${ConfigLoader.epidemicApiUrl}")
        val url = new URL(ConfigLoader.epidemicApiUrl)
        val conn = url.openConnection().asInstanceOf[HttpURLConnection]
        conn.setConnectTimeout(ConfigLoader.apiTimeout)
        conn.setReadTimeout(ConfigLoader.apiTimeout)
        conn.setRequestMethod("GET")

        val inputStream = conn.getInputStream
        val json = Source.fromInputStream(inputStream).mkString
        inputStream.close()

        // sauvegarde temporaire pour Spark
        val tmpPath = "data/tmp/epidemic.json"
        Files.createDirectories(Paths.get("data/tmp"))
        Files.write(Paths.get(tmpPath), json.getBytes)

        return spark.read.json(tmpPath)
      } catch {
        case e: IOException =>
          logger.error(s"Erreur API: ${e.getMessage}")
          lastEx = Some(e)
          attempt += 1
      }
    }
    throw lastEx.getOrElse(new RuntimeException("Erreur inconnue lors de la lecture API"))
  }

  // Écriture vers MySQL ou PostgreSQL
  private def writeToDB(df: DataFrame): Unit = {
    logger.info(s"Écriture vers ${ConfigLoader.dbType} table ${ConfigLoader.dbTable}")
    val driver = if (ConfigLoader.dbType == "mysql") "com.mysql.cj.jdbc.Driver"
                 else "org.postgresql.Driver"

    df.write
      .format("jdbc")
      .option("url", ConfigLoader.dbUrl)
      .option("dbtable", ConfigLoader.dbTable)
      .option("user", ConfigLoader.dbUser)
      .option("password", ConfigLoader.dbPassword)
      .option("driver", driver)
      .mode("append")
      .save()
  }
}

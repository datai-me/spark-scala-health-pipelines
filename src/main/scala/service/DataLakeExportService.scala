package service

import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import java.util.concurrent.TimeoutException

object DataLakeExportService {

  private val logger = LogManager.getLogger(getClass.getName)

  /**
   * Lance l'export Data Lake (Parquet/ORC/JSON + CSV analyst).
   *
   * @param spark SparkSession
   * @param df DataFrame à écrire
   * @param datasetName nom logique du dataset (équivalent "nom de table" dans le lake)
   * @param awaitCompletion true = attend la fin (recommandé si spark.stop() arrive juste après)
   * @param timeout durée max d'attente si awaitCompletion=true
   */
  def exportDataLake(
      spark: SparkSession,
      df: DataFrame,
      datasetName: String,
      awaitCompletion: Boolean = true,
      timeout: FiniteDuration = 10.minutes
  )(implicit ec: ExecutionContext): Unit = {

    logger.info(s"[DataLakeExport] START dataset=$datasetName await=$awaitCompletion timeout=${timeout.toSeconds}s")

    val writeFuture: Future[Unit] =
      DataLakeWriterAsync.writeBothAsync(spark, df, datasetName)

    // Logs fin/erreur même si on n'attend pas
    writeFuture.onComplete {
      case Success(_) =>
        logger.info(s"[DataLakeExport] DONE dataset=$datasetName")
      case Failure(e) =>
        logger.error(s"[DataLakeExport] FAIL dataset=$datasetName", e)
    }

    // Mode bloquant (sûr)
    if (awaitCompletion) {
      Try(Await.result(writeFuture, timeout)) match {
        case Success(_) =>
          logger.info(s"[DataLakeExport] COMPLETED (await) dataset=$datasetName")
        case Failure(_: TimeoutException) =>
          logger.warn(s"[DataLakeExport] TIMEOUT after ${timeout.toSeconds}s dataset=$datasetName (continuing)")
        case Failure(e) =>
          logger.error(s"[DataLakeExport] ERROR (await) dataset=$datasetName", e)
          throw e
      }
    }
  }
}

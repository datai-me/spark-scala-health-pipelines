package service

import config.ConfigLoader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.logging.log4j.LogManager

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object DataLakeWriterAsync {

  private val logger = LogManager.getLogger(getClass.getName)

  /**
   * Écrit un DataFrame en mode "data lake":
   * - MAIN: format colonne (Parquet/ORC/JSON) pour exploitation Big Data
   * - ANALYST: export CSV (ou Parquet) pour analystes
   *
   * Retourne un Future, mais ATTENTION:
   * il ne faut pas stopper Spark avant la fin des writes.
   */
  def writeBothAsync(
      spark: SparkSession,
      df: DataFrame,
      datasetName: String
  )(implicit ec: ExecutionContext): Future[Unit] = {

    val dfWithMeta = addStandardColumns(df)

    val mainPath = s"${ConfigLoader.dataLakeBasePath}/main/$datasetName"
    val analystPath = s"${ConfigLoader.dataLakeBasePath}/analyst/$datasetName"

    val fMain = Future {
      writeMain(dfWithMeta, mainPath)
    }

    val fAnalyst = Future {
      writeAnalyst(dfWithMeta, analystPath)
    }

    val combined = Future.sequence(Seq(fMain, fAnalyst)).map(_ => ())

    combined.onComplete {
      case Success(_) =>
        logger.info(s"DataLake write OK for dataset=$datasetName (main + analyst)")
      case Failure(e) =>
        logger.error(s"DataLake write FAILED for dataset=$datasetName", e)
    }

    combined
  }

  /** Colonnes standards utiles aux analystes et au partitionnement */
  private def addStandardColumns(df: DataFrame): DataFrame = {
    df.withColumn("ingest_ts", current_timestamp())
      .withColumn("ingest_date", to_date(col("ingest_ts")))
  }

  /** MAIN: Parquet/ORC/JSON (Hadoop-friendly) */
  private def writeMain(df: DataFrame, path: String): Unit = {
    logger.info(s"Writing MAIN dataset to $path as ${ConfigLoader.dlFormatMain}")

    val writer = df.write.mode(ConfigLoader.dlMode)

    val writerPartitioned =
      if (ConfigLoader.dlPartitionCols.nonEmpty)
        writer.partitionBy(ConfigLoader.dlPartitionCols: _*)
      else writer

    ConfigLoader.dlFormatMain.toLowerCase match {
      case "parquet" =>
        writerPartitioned
          .option("compression", "snappy")
          .parquet(path)

      case "orc" =>
        writerPartitioned.orc(path)

      case "json" =>
        writerPartitioned.json(path)

      case other =>
        throw new IllegalArgumentException(s"Unsupported DL_FORMAT_MAIN=$other (parquet|orc|json)")
    }
  }

  /** ANALYST: CSV propre (Excel-friendly) */
  private def writeAnalyst(df: DataFrame, path: String): Unit = {
    logger.info(s"Writing ANALYST dataset to $path as ${ConfigLoader.dlFormatAnalyst}")

    ConfigLoader.dlFormatAnalyst.toLowerCase match {
      case "csv" =>
        // coalesce=1 -> un seul fichier CSV (pratique pour analystes)
        // (sur gros volumes, préfère coalesce=0 / ou un nombre >1)
        df.coalesce(ConfigLoader.dlCoalesceAnalyst)
          .write.mode("overwrite") // souvent mieux en export analyst
          .option("header", "true")
          .option("delimiter", ",")
          .option("quote", "\"")
          .option("escape", "\"")
          .option("nullValue", "")
          .csv(path)

      case "parquet" =>
        df.write.mode("overwrite").parquet(path)

      case other =>
        throw new IllegalArgumentException(s"Unsupported DL_FORMAT_ANALYST=$other (csv|parquet)")
    }
  }
}

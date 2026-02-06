// =============================================================
// PROJET : EPIDEMIC BIG DATA PIPELINE
// STACK  : Scala 2.13 | Spark 3.x | Akka HTTP | MLlib
// CAS    : Surveillance épidémiologique à partir d'une API publique
// =============================================================

package pipeline

import org.apache.spark.sql.SparkSession
import config.{ConfigLoader, MLConfig}
import utils.{Logging, VersionsInfo}
import service.EpidemicApiServiceAsync

import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}
import scala.concurrent.{Await, Future}
import java.util.concurrent.TimeoutException

/**
 * Point d'entrée principal de l'application Epidemic Pipeline
 * 
 * Workflow:
 * 1. Initialisation de l'environnement Spark
 * 2. Affichage des versions des dépendances
 * 3. Configuration du logging
 * 4. Exécution du pipeline de données
 * 5. Nettoyage des ressources (graceful shutdown)
 * 
 * Usage:
 * {{{
 *   sbt run
 *   # ou avec arguments
 *   sbt "run --enable-cv true"
 * }}}
 * 
 * Variables d'environnement supportées:
 * - SPARK_MASTER: master URL (default: local[*])
 * - SPARK_APP_NAME: nom de l'application
 * - ML_ENABLE_CV: activer cross-validation
 * - API_MAX_RETRIES: nombre de tentatives API
 * 
 * @see [[config.ConfigLoader]] pour la configuration complète
 * @see [[config.MLConfig]] pour les paramètres ML
 */
object Main extends Logging {

  /**
   * Point d'entrée de l'application
   * 
   * @param args Arguments en ligne de commande (optionnel)
   *             Formats supportés:
   *             --enable-cv true/false
   *             --log-level DEBUG/INFO/WARN
   */
  def main(args: Array[String]): Unit = {
    
    // 🔥 Force le chargement de log4j2.xml
    System.setProperty(
      "log4j.configurationFile",
      getClass.getClassLoader.getResource("log4j2.xml").toURI.toString
    )
    
    logger.info("=" * 60)
    logger.info("Starting Epidemic Big Data Pipeline")
    logger.info("=" * 60)

    // Parse des arguments en ligne de commande
    val config = parseArguments(args)
    
    // Configuration du logging selon le niveau demandé
    config.get("log-level").foreach { level =>
      utils.Logging.setLogLevel("pipeline", level)
      utils.Logging.setLogLevel("service", level)
    }

    // Réduire le bruit des librairies tierces
    utils.Logging.silenceThirdPartyLibs()

    // Initialisation de Spark avec configuration optimisée
    val spark = initializeSpark()

    // Ajout d'un shutdown hook pour nettoyage gracieux
    addShutdownHook(spark)

    // Affichage des versions de l'environnement
    VersionsInfo.printVersions(spark)
    
    // Affichage de la configuration ML
    if (logger.isDebugEnabled) {
      MLConfig.printConfig()
    }

    // Exécution du pipeline avec gestion d'erreurs
    val result = Try {
      timed("Complete pipeline execution") {
        val enableCV = config.get("enable-cv").exists(_.toBoolean)
        EpidemicPipelineApp.run(spark, enableCrossValidation = enableCV)
      }
    }

    // Traitement du résultat
    result match {
      case Success(_) =>
        logger.info("=" * 60)
        logger.info("Pipeline completed successfully! ✓")
        logger.info("=" * 60)
        
      case Failure(exception) =>
        logger.error("=" * 60)
        logger.error("Pipeline failed with error", exception)
        logger.error("=" * 60)
        
        // Cleanup async resources avant de quitter
        cleanupAsyncResources()
        
        // Arrêt de Spark
        stopSpark(spark)
        
        // Exit avec code d'erreur
        System.exit(1)
    }

    // Nettoyage normal des ressources
    cleanupResources(spark)
    
    logger.info("Application terminated gracefully")
  }

  /**
   * Initialise la SparkSession avec configuration optimisée
   * 
   * Configurations appliquées:
   * - Adaptive Query Execution (AQE) pour optimiser les plans
   * - Dynamic allocation pour gérer les resources dynamiquement
   * - Serialization Kryo pour meilleures performances
   * - Compression Snappy pour économiser l'espace
   * 
   * @return SparkSession configurée
   */
  private def initializeSpark(): SparkSession = {
    logger.info("Initializing Spark session")

    // Configuration Hadoop (Windows uniquement)
    if (System.getProperty("os.name").toLowerCase.contains("win")) {
      logger.debug("Detected Windows OS, setting Hadoop home directory")
      System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
    }

    val spark = SparkSession.builder()
      .appName(ConfigLoader.appName)
      .master(ConfigLoader.master)
      // Optimisations Spark SQL
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.shuffle.partitions", "200")
      // Optimisations mémoire
      .config("spark.default.parallelism", "100")
      .config("spark.sql.autoBroadcastJoinThreshold", "10485760") // 10MB
      // Serialization
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrationRequired", "false")
      // Compression
      .config("spark.sql.inMemoryColumnarStorage.compressed", "true")
      .config("spark.sql.inMemoryColumnarStorage.batchSize", "10000")
      // UI et monitoring
      .config("spark.ui.showConsoleProgress", "true")
      .config("spark.sql.execution.arrow.pyspark.enabled", "true")
      .getOrCreate()

    // Réduire le niveau de log Spark (trop verbeux par défaut)
    spark.sparkContext.setLogLevel("WARN") // INFO SI ON VEUT VOIR TOUS LES LOGS.

    logger.info(s"Spark session initialized - Version: ${spark.version}")
    logger.info(s"Master: ${spark.sparkContext.master}")
    logger.info(s"App ID: ${spark.sparkContext.applicationId}")
    
    spark
  }

  /**
   * Parse les arguments en ligne de commande
   * 
   * Format attendu: --key value
   * 
   * @param args Arguments bruts
   * @return Map des configurations
   */
  private def parseArguments(args: Array[String]): Map[String, String] = {
    logger.debug(s"Parsing arguments: ${args.mkString(" ")}")
    
    args.sliding(2, 2).collect {
      case Array(key, value) if key.startsWith("--") =>
        val cleanKey = key.stripPrefix("--")
        logger.debug(s"Argument: $cleanKey = $value")
        cleanKey -> value
    }.toMap
  }

  /**
   * Ajoute un shutdown hook pour nettoyage en cas d'arrêt brutal
   * 
   * Capture SIGTERM, SIGINT (Ctrl+C) et arrêt JVM
   */
  private def addShutdownHook(spark: SparkSession): Unit = {
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = {
        logger.warn("Shutdown hook triggered - cleaning up resources (best-effort)")

        Try {
          cleanupAsyncResources()
          stopSpark(spark)
        } match {
          case Success(_) => 
            logger.info("Cleanup completed successfully")
          case Failure(ex) => 
            logger.error("Error during cleanup", ex)
        }
      }
    })
    
    logger.debug("Shutdown hook registered")
  }

  /**
   * Nettoie toutes les ressources de l'application
   */
  private def cleanupResources(spark: SparkSession): Unit = {
    logger.info("Starting cleanup of resources")
    
    // Cleanup async resources
    cleanupAsyncResources()
    
    // Arrêt de Spark
    stopSpark(spark)
    
    logger.info("All resources cleaned up successfully")
  }

    /**
   * Nettoie les ressources asynchrones (best-effort)
   *
   * IMPORTANT:
   * - Ne doit jamais faire échouer l'arrêt de l'application.
   * - Ne doit pas bloquer trop longtemps (sinon Timeout).
   */
  private def cleanupAsyncResources(): Unit = {
    logger.info("Cleaning up async resources (best-effort)")

    val shutdownFuture: Future[Unit] = shutdownAsyncResources()

    // Best-effort: on attend un peu, puis on abandonne sans planter l'arrêt
    Try(Await.result(shutdownFuture, 10.seconds)) match {
      case Success(_) =>
        logger.info("Async resources shut down cleanly")

      case Failure(_: TimeoutException) =>
        logger.warn("Async shutdown timed out after 10s; continuing shutdown anyway")

      case Failure(e) =>
        logger.warn("Async shutdown failed; continuing shutdown anyway", e)
    }
  }

  /**
   * Tente d'arrêter proprement les ressources async.
   *
   * - Si EpidemicApiServiceAsync expose une méthode shutdown(): Future[Unit], on l'appelle.
   * - Sinon, on retourne Future.successful(()) pour ne pas bloquer.
   *
   * Cette approche évite les erreurs de compilation si l'API async change,
   * et garantit un arrêt "safe" sous Windows/Docker.
   */
  private def shutdownAsyncResources(): Future[Unit] = {
    Try {
      val m = EpidemicApiServiceAsync.getClass.getMethod("shutdown")
      m.invoke(EpidemicApiServiceAsync).asInstanceOf[Future[Unit]]
    }.getOrElse(Future.successful(()))
  }


  /**
   * Arrête proprement la SparkSession
   */
  private def stopSpark(spark: SparkSession): Unit = {
    if (!spark.sparkContext.isStopped) {
      logger.info("Stopping Spark session")
      spark.stop()
      logger.info("Spark session stopped")
    }
  }
}
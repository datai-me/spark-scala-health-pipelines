package utils

import org.apache.logging.log4j.{LogManager, Logger}

/**
 * Trait de logging réutilisable pour tout le projet.
 *
 * Version Spark-friendly:
 * - Utilise Log4j2 (natif Spark 3.x)
 * - Aucune dépendance Logback (ch.qos.logback.* supprimée)
 *
 * Configuration:
 * - log4j2.xml dans src/main/resources
 */
trait Logging {

  /**
   * Logger spécifique à la classe qui utilise ce trait.
   * @transient + lazy pour éviter les problèmes de sérialisation Spark.
   */
  @transient protected lazy val logger: Logger =
    LogManager.getLogger(getClass.getName)

  /**
   * Log un message de debug avec contexte structuré.
   */
  protected def logDebugWithContext(
      message: String,
      context: Map[String, Any]
  ): Unit = {
    if (logger.isDebugEnabled) {
      val contextStr = context.map { case (k, v) => s"$k=$v" }.mkString(", ")
      logger.debug(s"$message | $contextStr")
    }
  }

  /**
   * Log un message d'info avec contexte structuré.
   */
  protected def logInfoWithContext(
      message: String,
      context: Map[String, Any]
  ): Unit = {
    if (logger.isInfoEnabled) {
      val contextStr = context.map { case (k, v) => s"$k=$v" }.mkString(", ")
      logger.info(s"$message | $contextStr")
    }
  }

  /**
   * Log une erreur avec contexte et stack trace.
   */
  protected def logErrorWithContext(
      message: String,
      exception: Throwable,
      context: Map[String, Any] = Map.empty
  ): Unit = {
    val contextStr =
      if (context.nonEmpty)
        " | " + context.map { case (k, v) => s"$k=$v" }.mkString(", ")
      else
        ""
    logger.error(s"$message$contextStr", exception)
  }

  /**
   * Mesure le temps d'exécution d'un bloc de code et log le résultat.
   */
  protected def timed[T](operationName: String)(block: => T): T = {
    val startTime = System.currentTimeMillis()

    try {
      val result = block
      val duration = System.currentTimeMillis() - startTime
      logger.info(s"$operationName completed in ${duration}ms")
      result
    } catch {
      case ex: Throwable =>
        val duration = System.currentTimeMillis() - startTime
        logger.error(s"$operationName failed after ${duration}ms", ex)
        throw ex
    }
  }

  /**
   * Log conditionnel basé sur un seuil de performance.
   * Utile pour détecter les opérations lentes.
   */
  protected def logIfSlow[T](
      operationName: String,
      thresholdMs: Long = 1000
  )(block: => T): T = {
    val startTime = System.currentTimeMillis()
    val result = block
    val duration = System.currentTimeMillis() - startTime

    if (duration > thresholdMs) {
      logger.warn(
        s"SLOW OPERATION: $operationName took ${duration}ms (threshold: ${thresholdMs}ms)"
      )
    } else {
      logger.debug(s"$operationName completed in ${duration}ms")
    }

    result
  }
}

/**
 * Objet companion avec des utilitaires de logging.
 * Version Log4j2 (sans Logback).
 */
object Logging {

  /**
   * Change le niveau de log d'un logger à la volée (Log4j2).
   *
   * Nécessite log4j-core sur le classpath (ce qui est généralement le cas si tu as log4j2).
   */
  def setLogLevel(loggerName: String, level: String): Unit = {
    try {
      val lvl = org.apache.logging.log4j.Level.valueOf(level.toUpperCase)
      // Configurator est dans log4j-core
      org.apache.logging.log4j.core.config.Configurator.setLevel(loggerName, lvl)
      println(s"Set log level for $loggerName to $lvl")
    } catch {
      case _: IllegalArgumentException =>
        println(s"Invalid log level: $level (use TRACE, DEBUG, INFO, WARN, ERROR)")
      case _: Throwable =>
        // Si log4j-core n'est pas présent ou autre souci
        println(s"Cannot set log level dynamically for $loggerName (log4j-core required).")
    }
  }

  def enableDebugMode(): Unit = {
    setLogLevel("pipeline", "DEBUG")
    setLogLevel("service", "DEBUG")
    setLogLevel("ingestion", "DEBUG")
    setLogLevel("mlops", "DEBUG")
    println("Debug mode enabled for all project packages")
  }

  def silenceThirdPartyLibs(): Unit = {
    setLogLevel("org.apache.spark", "WARN")
    setLogLevel("org.apache.hadoop", "WARN")
    setLogLevel("org.sparkproject", "WARN")
    println("Third-party library logging reduced to WARN")
  }
}

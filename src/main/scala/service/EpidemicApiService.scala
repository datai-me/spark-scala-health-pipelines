package service

import pipeline.config.ConfigLoader
import pipeline.util.Logging
import api.ApiClient

import scala.util.{Try, Success, Failure}
import scala.concurrent.duration._

/**
 * EpidemicApiService
 *
 * - Utilise ApiClient pour appeler l'API REST
 * - Gère retry + backoff exponentiel
 * - Retourne le JSON brut (String)
 */
object EpidemicApiService extends Logging {

  /**
   * Appel principal de l'API épidémie
   */
  def fetchEpidemicJson(): String = {
    retryWithBackoff(
      maxRetries = ConfigLoader.apiMaxRetries,
      initialDelay = ConfigLoader.apiInitialBackoffMs.millis
    ) {
      logger.info(s"Calling epidemic API: ${ConfigLoader.epidemicApiUrl}")
      ApiClient.get(ConfigLoader.epidemicApiUrl)
    }
  }

  /**
   * Retry générique avec backoff exponentiel
   */
  private def retryWithBackoff[T](
      maxRetries: Int,
      initialDelay: FiniteDuration
  )(block: => T): T = {

    var attempt = 0
    var delay = initialDelay
    var lastError: Throwable = null

    while (attempt < maxRetries) {
      Try(block) match {
        case Success(result) =>
          logger.info(s"API call succeeded on attempt ${attempt + 1}")
          return result

        case Failure(ex) =>
          attempt += 1
          lastError = ex
          logger.warn(
            s"API call failed (attempt $attempt/$maxRetries). Retrying in ${delay.toMillis} ms",
            ex
          )
          Thread.sleep(delay.toMillis)
          delay = delay * 2
      }
    }

    logger.error("API call failed after max retries", lastError)
    throw lastError
  }
}

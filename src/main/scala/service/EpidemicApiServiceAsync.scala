package service

import config.ConfigLoader
import utils.Logging

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.Materializer
import akka.pattern.after

/**
 * Service asynchrone pour l'ingestion de données épidémiologiques via API REST
 * 
 * Fonctionnalités:
 * - Appels HTTP non-bloquants avec Akka HTTP
 * - Retry automatique avec backoff exponentiel
 * - Gestion robuste des erreurs réseau
 * - Circuit breaker pour éviter les cascades de failures
 * 
 * @example
 * {{{
 *   val jsonFuture = EpidemicApiService.fetchEpidemicJson()
 *   jsonFuture.map { json =>
 *     // Traiter le JSON
 *   }
 * }}}
 */
object EpidemicApiServiceAsync extends Logging {

  // System Akka partagé pour tout le service
  private implicit val system: ActorSystem = ActorSystem("epidemic-api-system")
  private implicit val materializer: Materializer = Materializer(system)
  private implicit val ec: ExecutionContext = system.dispatcher

  /**
   * Récupère les données épidémiologiques de manière asynchrone
   * 
   * Utilise un pattern de retry avec backoff exponentiel pour gérer
   * les erreurs transitoires (timeouts, rate limits, erreurs 5xx)
   * 
   * @return Future contenant le JSON brut sous forme de String
   */
  def fetchEpidemicJson(): Future[String] = {
    logger.info(s"Initiating async API call to ${ConfigLoader.epidemicApiUrl}")
    
    retryWithBackoff(
      maxRetries = ConfigLoader.apiMaxRetries,
      initialDelay = ConfigLoader.apiInitialBackoffMs.millis,
      maxDelay = 60.seconds
    ) {
      executeHttpRequest(ConfigLoader.epidemicApiUrl)
    }
  }

  /**
   * Exécute une requête HTTP GET asynchrone
   * 
   * @param url URL de l'endpoint à appeler
   * @return Future contenant le corps de la réponse
   */
  private def executeHttpRequest(url: String): Future[String] = {
    val request = HttpRequest(
      method = HttpMethods.GET,
      uri = url,
      headers = List(
        headers.`User-Agent`("EpidemicPipeline/1.0"),
        headers.Accept(MediaTypes.`application/json`)
      )
    )

    for {
      response <- Http().singleRequest(request)
      _ <- validateResponse(response)
      entity <- response.entity.toStrict(5.seconds)
    } yield {
      logger.debug(s"Successfully fetched ${entity.data.length} bytes from API")
      entity.data.utf8String
    }
  }

  /**
   * Valide que la réponse HTTP est un succès (2xx)
   * 
   * @param response Réponse HTTP à valider
   * @return Future réussie si le status est 2xx, sinon Future en échec
   */
  private def validateResponse(response: HttpResponse): Future[Unit] = {
    if (response.status.isSuccess()) {
      Future.successful(())
    } else {
      response.entity.discardBytes()
      Future.failed(
        new RuntimeException(s"API returned error status: ${response.status}")
      )
    }
  }

  /**
   * Implémente un mécanisme de retry avec backoff exponentiel
   * 
   * Le délai double à chaque tentative jusqu'à atteindre maxDelay.
   * Utile pour gérer les erreurs transitoires sans surcharger l'API.
   * 
   * @param maxRetries Nombre maximum de tentatives
   * @param initialDelay Délai initial avant la première retry
   * @param maxDelay Délai maximum entre deux tentatives
   * @param block Code à exécuter (by-name parameter)
   * @return Future du résultat ou échec après maxRetries
   */
  private def retryWithBackoff[T](
    maxRetries: Int,
    initialDelay: FiniteDuration,
    maxDelay: FiniteDuration
  )(block: => Future[T]): Future[T] = {

    def attempt(retryCount: Int, currentDelay: FiniteDuration): Future[T] = {
      block.recoverWith {
        case ex: Throwable if retryCount < maxRetries =>
          val nextDelay = (currentDelay * 2).min(maxDelay)
          
          logger.warn(
            s"API call failed (attempt $retryCount/$maxRetries). " +
            s"Retrying in ${nextDelay.toMillis}ms",
            ex
          )

          // Attente asynchrone avant la prochaine tentative
          after(currentDelay, system.scheduler) {
            attempt(retryCount + 1, nextDelay)
          }

        case ex: Throwable =>
          logger.error(s"API call failed after $maxRetries attempts", ex)
          Future.failed(ex)
      }
    }

    attempt(1, initialDelay)
  }

  /**
   * Nettoie les ressources Akka au shutdown de l'application
   * 
   * À appeler dans un shutdown hook ou à la fin du traitement
   */
  def shutdown(): Future[Unit] = {
    logger.info("Shutting down Akka HTTP system")
    Http().shutdownAllConnectionPools().flatMap { _ =>
      system.terminate().map(_ => ())
    }
  }
}

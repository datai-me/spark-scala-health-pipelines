package utils

import org.slf4j.{Logger, LoggerFactory}

/**
 * Trait Logging
 *
 * À étendre dans les services, repositories, etc.
 * Fournit un logger SLF4J prêt à l'emploi.
 *
 * Exemple :
 * object MyService extends Logging {
 *   logger.info("Hello world")
 * }
 */
trait Logging {

  // Logger associé à la classe concrète
  protected lazy val logger: Logger =
    LoggerFactory.getLogger(this.getClass)

}

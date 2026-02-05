package persistence

/**
 * Contient les requêtes SQL spécifiques à MySQL 8
 * Utilisé pour faire un UPSERT (MERGE) depuis la table staging
 *
 * Clé primaire : (country, updated)
 */
object MySql {

  /**
   * UPSERT MySQL 8
   * Utilise ON DUPLICATE KEY UPDATE
   */
  val mergeSql: String =
    """
      |INSERT INTO epidemic_cases (
      |  country,
      |  updated,
      |  iso2,
      |  iso3,
      |  continent,
      |  population,
      |  cases,
      |  deaths,
      |  recovered,
      |  active,
      |  critical
      |)
      |SELECT
      |  country,
      |  updated,
      |  iso2,
      |  iso3,
      |  continent,
      |  population,
      |  cases,
      |  deaths,
      |  recovered,
      |  active,
      |  critical
      |FROM epidemic_cases_staging
      |ON DUPLICATE KEY UPDATE
      |  iso2 = VALUES(iso2),
      |  iso3 = VALUES(iso3),
      |  continent = VALUES(continent),
      |  population = VALUES(population),
      |  cases = VALUES(cases),
      |  deaths = VALUES(deaths),
      |  recovered = VALUES(recovered),
      |  active = VALUES(active),
      |  critical = VALUES(critical);
    """.stripMargin
}

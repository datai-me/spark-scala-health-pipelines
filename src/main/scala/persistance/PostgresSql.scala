package persistence

object PostgresSql {

  val mergeSql: String =
    """
      |INSERT INTO epidemic_cases (
      |  country, updated, iso2, iso3, continent, population,
      |  cases, deaths, recovered, active, critical
      |)
      |SELECT
      |  country, updated, iso2, iso3, continent, population,
      |  cases, deaths, recovered, active, critical
      |FROM epidemic_cases_staging
      |ON CONFLICT (country, updated)
      |DO UPDATE SET
      |  cases = EXCLUDED.cases,
      |  deaths = EXCLUDED.deaths,
      |  recovered = EXCLUDED.recovered,
      |  active = EXCLUDED.active,
      |  critical = EXCLUDED.critical;
    """.stripMargin
}

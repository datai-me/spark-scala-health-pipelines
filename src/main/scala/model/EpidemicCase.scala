package model

/**
 * Modèle métier EXACT du JSON retourné par l'API disease.sh
 * Utilisé après parsing Spark SQL
 */
case class EpidemicCase(
  updated: Long,              // timestamp API
  country: String,
  iso2: String,
  iso3: String,
  continent: String,
  population: Long,

  cases: Long,
  todayCases: Long,
  deaths: Long,
  todayDeaths: Long,
  recovered: Long,
  active: Long,
  critical: Long
)

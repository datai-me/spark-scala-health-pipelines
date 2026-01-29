package model

/**
 * Modèle métier représentant les données d’un pays
 * Mapping direct du JSON API
 */
case class EpidemicCountry(
  country: String,
  cases: Long,
  deaths: Long,
  recovered: Long,
  population: Long
)

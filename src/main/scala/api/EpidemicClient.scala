package api

import requests._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import model.EpidemicCountry

/**
 * Client REST pour l’API publique disease.sh
 */
object EpidemicApiClient {

  private val apiUrl =
    "https://disease.sh/v3/covid-19/countries"

  /**
   * Appel HTTP GET vers l’API
   */
  def fetchRawJson(): String = {
    val response = requests.get(apiUrl)
    response.text()
  }

  /**
   * Parsing JSON → objets Scala
   */
  def parse(json: String): Seq[EpidemicCountry] = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    mapper
      .readValue(json, classOf[Array[EpidemicCountry]])
      .toSeq
  }
}

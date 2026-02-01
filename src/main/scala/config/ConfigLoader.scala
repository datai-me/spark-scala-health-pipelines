package config

import com.typesafe.config.{Config, ConfigFactory}

object ConfigLoader {

  // Charge application.conf depuis resources
  private val config: Config = ConfigFactory.load()

  // 🔹 Spark
  val appName: String =
    config.getString("spark.app-name")

  val master: String =
    config.getString("spark.master")

  // 🔹 API Epidemic
  val epidemicApiUrl: String =
    config.getString("epidemic.api.url")

  // 🔹 PostgreSQL
  val postgresUrl: String =
    config.getString("postgres.url")

  val postgresUser: String =
    config.getString("postgres.user")

  val postgresPassword: String =
    config.getString("postgres.password")
}

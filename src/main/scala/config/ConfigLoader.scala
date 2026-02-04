package config

import com.typesafe.config.{Config, ConfigFactory}

object ConfigLoader {

  // 🔹 Spark
  val appName: String = sys.env.getOrElse("SPARK_APP_NAME", "Epidemic Big Data Pipeline")
  val master: String  = sys.env.getOrElse("SPARK_MASTER", "local[*]")
 
  // 🔹 API Epidemic
  val epidemicApiUrl: String = sys.env.getOrElse(
    "EPIDEMIC_API_URL",
    "https://disease.sh/v3/covid-19/countries"
  )
  val apiRetries: Int = sys.env.getOrElse("EPIDEMIC_API_RETRIES", "3").toInt
  val apiTimeout: Int = sys.env.getOrElse("EPIDEMIC_API_TIMEOUT", "10000").toInt

  // 🔹 PostgreSQL 
  val postgresUrl: String      = sys.env.getOrElse(
    "POSTGRES_URL",
    "jdbc:postgresql://localhost:5432/epidemic_db?currentSchema=public&ssl=false"
  )
  val postgresTable: String    = sys.env.getOrElse("POSTGRES_TABLE", "epidemic_cases")
  val postgresUser: String     = sys.env.getOrElse("POSTGRES_USER", "postgres")
  val postgresPassword: String = sys.env.getOrElse("POSTGRES_PASSWORD", "postgres")
  
   // 🔹 MySQL
  val mysqlUrl: String      = sys.env.getOrElse(
    "MYSQL_URL",
    "jdbc:mysql://localhost:3306/epidemic_db?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"
  )
  val mysqlTable: String    = sys.env.getOrElse("MYSQL_TABLE", "epidemic_cases")
  val mysqlUser: String     = sys.env.getOrElse("MYSQL_USER", "root")
  val mysqlPassword: String = sys.env.getOrElse("MYSQL_PASSWORD", "Pa$$w0rd")
}

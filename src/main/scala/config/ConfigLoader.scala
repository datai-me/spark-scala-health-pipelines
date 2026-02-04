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

  // =========================
  // 🔹DATABASE CONFIG
  // =========================
  val dbType: String = sys.env.getOrElse("DB_TYPE", "mysql") // mysql ou postgres

  val dbUrl: String = sys.env.getOrElse(
    "DB_URL",
    if(dbType == "mysql") 
      "jdbc:mysql://localhost:3306/epidemic_db?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"
    else
      "jdbc:postgresql://localhost:5432/epidemic_db?currentSchema=public&ssl=false"
  )
  val dbUser: String = sys.env.getOrElse(
    "DB_USER",
    if(dbType == "mysql") "root" else "postgres"
  )

  val dbPassword: String = sys.env.getOrElse(
    "DB_PASSWORD",
    if(dbType == "mysql") "Pa$$w0rd" else "postgres"
  )
  
   val dbTable: String = sys.env.getOrElse("DB_TABLE", "epidemic_cases")
}

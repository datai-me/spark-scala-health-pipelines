package config

object ConfigLoader {

  // 🔹 Spark
  val appName = sys.env.getOrElse("SPARK_APP_NAME", "Epidemic Pipeline")
  val master  = sys.env.getOrElse("SPARK_MASTER", "local[*]")
  
  // 🔹 API Epidemic
  val epidemicApiUrl: String = sys.env.getOrElse(
    "EPIDEMIC_API_URL",
    "https://disease.sh/v3/covid-19/countries"
  )  
  
  val apiMaxRetries: Int = sys.env.getOrElse("API_MAX_RETRIES", "3").toInt
  val apiInitialBackoffMs: Int = sys.env.getOrElse("API_INITIAL_BACKOFF_MS", "10000").toInt

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
  
  // JDBC générique (MySQL ou PostgreSQL)
  val jdbcUrl      = sys.env.getOrElse("JDBC_URL","jdbc:mysql://localhost:3306/epidemic_db?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true")
  val jdbcUser     = sys.env.getOrElse("JDBC_USER","root")
  val jdbcPassword = sys.env.getOrElse("JDBC_PASSWORD","Pa$$w0rd")
  val jdbcTable    = sys.env.getOrElse("JDBC_TABLE", "epidemic_cases")
  val jdbcDriver   = sys.env.getOrElse("JDBC_DRIVER","com.mysql.cj.jdbc.Driver")

  // =========================
  // 🔹FILES CONFIG
  // =========================

  val dataLakeBasePath: String =
    sys.env.getOrElse("DATA_LAKE_BASE_PATH", "file:///C:/data-lake/epidemic")

  val dlFormatMain: String =
    sys.env.getOrElse("DL_FORMAT_MAIN", "parquet") // parquet|orc|json

  val dlFormatAnalyst: String =
    sys.env.getOrElse("DL_FORMAT_ANALYST", "csv") // csv|parquet

  val dlMode: String =
    sys.env.getOrElse("DL_MODE", "overwrite") // overwrite|append

  val dlPartitionCols: Seq[String] =
    sys.env.getOrElse("DL_PARTITION_COLS", "")
      .split(",").map(_.trim).filter(_.nonEmpty).toSeq

  val dlCoalesceAnalyst: Int =
    sys.env.getOrElse("DL_COALESCE_ANALYST", "1").toInt
  
}

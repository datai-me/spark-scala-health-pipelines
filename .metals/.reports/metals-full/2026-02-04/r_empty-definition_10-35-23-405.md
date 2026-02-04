error id: file:///C:/Users/user/Documents/GitHub/spark-scala-health-pipelines/src/main/scala/config/ConfigLoader.scala:scala/Predef.String#
file:///C:/Users/user/Documents/GitHub/spark-scala-health-pipelines/src/main/scala/config/ConfigLoader.scala
empty definition using pc, found symbol in pc: scala/Predef.String#
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -String#
	 -scala/Predef.String#
offset: 415
uri: file:///C:/Users/user/Documents/GitHub/spark-scala-health-pipelines/src/main/scala/config/ConfigLoader.scala
text:
```scala
package config

import com.typesafe.config.{Config, ConfigFactory}

object ConfigLoader {

  // Charge application.conf depuis resources
  private val config: Config = ConfigFactory.load()

  // 🔹 Spark
  val appName: String = config.getString("spark.app-name")
  val master: String = config.getString("spark.master")
  println(appName)
  println(master)
  // 🔹 API Epidemic
  val epidemicApiUrl: S@@tring = config.getString("epidemic.api.url")

  // 🔹 PostgreSQL
  val postgresUrl: String = config.getString("postgres.url")
  val postgresUser: String = config.getString("postgres.user")
  val postgresPassword: String =  config.getString("postgres.password")
  
   // 🔹 MySQL
  val mysqlUrl: String      = config.getString("mysql.url")
  val mysqlTable: String    = config.getString("mysql.table")
  val mysqlUser: String     = config.getString("mysql.user")
  val mysqlPassword: String = config.getString("mysql.password")
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: scala/Predef.String#
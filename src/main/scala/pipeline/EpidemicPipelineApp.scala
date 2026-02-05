// -------------------------------------------------------------
// FICHIER : pipeline/EpidemicPipelineApp.scala
// RÔLE    : Point d'entrée du pipeline
// -------------------------------------------------------------

package pipeline

import org.apache.spark.sql.SparkSession
import ingestion.EpidemicApiIngestion
import config.ConfigLoader

object EpidemicPipelineApp {

  def run(spark: SparkSession): Unit = {

    val df = EpidemicApiIngestion.readApi(spark)

    // Affichage console Spark SQL
    df.createOrReplaceTempView("epidemic")
    spark.sql("SELECT country, cases, deaths FROM epidemic").show(20, false)

    // Écriture JDBC
    df.write
      .format("jdbc")
      .option("url", ConfigLoader.jdbcUrl)
      .option("dbtable", ConfigLoader.jdbcTable)
      .option("user", ConfigLoader.jdbcUser)
      .option("password", ConfigLoader.jdbcPassword)
      .option("driver", ConfigLoader.jdbcDriver)
      .mode("append")
      .save()
  }
}

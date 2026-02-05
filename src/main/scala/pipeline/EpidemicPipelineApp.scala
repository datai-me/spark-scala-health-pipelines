// -------------------------------------------------------------
// FICHIER : pipeline/EpidemicPipelineApp.scala
// RÔLE    : Point d'entrée du pipeline
// -------------------------------------------------------------

package pipeline

import org.apache.spark.sql.SparkSession
import ingestion.EpidemicApiIngestion
import config.ConfigLoader
import storage.DataWriter

object EpidemicPipelineApp {

  def run(spark: SparkSession): Unit = {

    val df = EpidemicApiIngestion.readApi(spark)

    // Affichage console Spark SQL
    df.createOrReplaceTempView("epidemic")
    spark.sql("SELECT country, cases, active, critical, population, deaths, tests, casesPerOneMillion, deathsPerOneMillion FROM epidemic").show(20, false)
	
    // 🚀 UN SEUL APPEL
    val (model, predictions) = MlPipeline.run(df)

    predictions.select("label", "prediction").show(10, false)

    // Sauvegarde modèle (MLOps)
    model.write.overwrite().save("/models/covid_rf")
	
    // Écriture JDBC
    // DataWriter.writeToJdbc(df)
  } 
}

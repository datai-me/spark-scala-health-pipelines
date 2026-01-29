// -------------------------------------------------------------
// FICHIER : config/SparkSessionBuilder.scala
// RÔLE    : Centralise la configuration Spark
// -------------------------------------------------------------

package config

import org.apache.spark.sql.SparkSession

/**
 * Fournisseur unique de SparkSession
 * (pattern singleton)
 */
object SparkSessionBuilder {

  /**
   * Crée et configure une SparkSession.   * 
   * - Mode local pour exécution Windows
   */
  // - Active Delta Lake
  def build(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]") // utilise tous les cœurs disponibles
	  //.config("spark.resources.discoveryPlugin", "") // pas recommandé (bricolage)
      //.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      //.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
	  .config("spark.driver.extraJavaOptions", "--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
      .config("spark.executor.extraJavaOptions", "--add-exports=java.base/java.nio=ALL-UNNAMED --add-exports=java.base/sun.nio.ch=ALL-UNNAMED")
      .getOrCreate()
  }
}
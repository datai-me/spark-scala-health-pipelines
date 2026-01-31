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
      .master("local[*]") // utilise tous les cœurs disponibles, à retirer en production
      //.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      //.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")	  
      .getOrCreate()
  }
}
package config

import org.apache.spark.sql.SparkSession

/**
 * Fournisseur unique de SparkSession
 * (pattern singleton)
 */
object SparkSessionProvider {

  lazy val spark: SparkSession =
    SparkSession.builder()
      .appName("Epidemic Spark SQL Pipeline")
      .master("local[*]") // à retirer en production
      //.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      //.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")	  
      .getOrCreate()
}

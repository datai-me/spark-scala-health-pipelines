error id: file:///C:/Users/user/Documents/GitHub/spark-scala-health-pipelines/src/main/scala/pipeline/Main.scala:info.
file:///C:/Users/user/Documents/GitHub/spark-scala-health-pipelines/src/main/scala/pipeline/Main.scala
empty definition using pc, found symbol in pc: info.
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -logger/info.
	 -logger/info#
	 -logger/info().
	 -scala/Predef.logger.info.
	 -scala/Predef.logger.info#
	 -scala/Predef.logger.info().
offset: 630
uri: file:///C:/Users/user/Documents/GitHub/spark-scala-health-pipelines/src/main/scala/pipeline/Main.scala
text:
```scala
// =============================================================
// PROJET : EPIDEMIC BIG DATA PIPELINE
// STACK  : Scala 2.13 | Spark 3.x | Delta Lake
// CAS    : Surveillance épidémiologique à partir d'une API publique
// =============================================================

package pipeline

import org.apache.spark.sql.SparkSession
import config.ConfigLoader
import utils.VersionsInfo
import pipeline.EpidemicPipelineApp
import org.apache.logging.log4j.LogManager

object Main {

 private val logger = LogManager.getLogger(Main.getClass)

  def main(args: Array[String]): Unit = {

    logger.inf@@o("Starting Epidemic Big Data Pipeline")

    // 🔹 Création UNIQUE du SparkSession
    val spark = SparkSession.builder()
      .appName(ConfigLoader.appName)
      .master("local[*]") // utilise tous les cœurs disponibles, à retirer en production
      //.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      //.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")	  
      .getOrCreate()

    // 🔹 Affichage des versions AU DÉMARRAGE
    VersionsInfo.printVersions(spark)

    // 🔹 Exécution du pipeline
    EpidemicPipelineApp.run(spark)

    // 🔹 Arrêt propre
    spark.stop()
    logger.info("Pipeline stopped successfully")
  }
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: info.
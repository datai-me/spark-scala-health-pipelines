error id: file:///C:/Users/user/Documents/GitHub/spark-scala-health-pipelines/src/main/scala/pipeline/EpidemicPipelineApp.scala:
file:///C:/Users/user/Documents/GitHub/spark-scala-health-pipelines/src/main/scala/pipeline/EpidemicPipelineApp.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -config/ConfigLoader.epidemicApiUrl.
	 -config/ConfigLoader.epidemicApiUrl#
	 -config/ConfigLoader.epidemicApiUrl().
	 -ConfigLoader.epidemicApiUrl.
	 -ConfigLoader.epidemicApiUrl#
	 -ConfigLoader.epidemicApiUrl().
	 -scala/Predef.ConfigLoader.epidemicApiUrl.
	 -scala/Predef.ConfigLoader.epidemicApiUrl#
	 -scala/Predef.ConfigLoader.epidemicApiUrl().
offset: 835
uri: file:///C:/Users/user/Documents/GitHub/spark-scala-health-pipelines/src/main/scala/pipeline/EpidemicPipelineApp.scala
text:
```scala
// -------------------------------------------------------------
// FICHIER : pipeline/EpidemicPipelineApp.scala
// RÔLE    : Point d'entrée du pipeline
// -------------------------------------------------------------

package pipeline

import org.apache.spark.sql.{DataFrame, SparkSession}
import config.ConfigLoader
import org.apache.logging.log4j.LogManager

object EpidemicPipelineApp {

  private val logger = LogManager.getLogger(EpidemicPipelineApp.getClass)

  def run(spark: SparkSession): Unit = {

    logger.info("Epidemic pipeline started")

    val df = loadEpidemicData(spark)
    df.show(false)

    logger.info("Epidemic pipeline finished")
  }

  private def loadEpidemicData(spark: SparkSession): DataFrame = {
    println()
    logger.info(s"Reading data from API: ${ConfigLoader.epidemic@@ApiUrl}")

    spark.read
      .format("json")
      .option("multiline", "true")
      .load(ConfigLoader.epidemicApiUrl)
  }
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: 
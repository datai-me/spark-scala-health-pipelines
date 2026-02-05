package mlops

import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.VectorAssembler

object FeatureEngineering {

  def prepare(df: DataFrame): DataFrame = {

    // Colonnes utilisées comme features
    val featureCols = Array(
      "cases",
      "active",
      "critical",
      "population",
      "tests",      
      "casesPerOneMillion",
      "deathsPerOneMillion"
    )

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    assembler.transform(df)
      .select("features", "deaths")
      .withColumnRenamed("deaths", "label")
  }

}

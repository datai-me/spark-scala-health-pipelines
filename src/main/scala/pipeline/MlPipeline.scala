package pipeline

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.evaluation.RegressionEvaluator

object MlPipeline {

  /**
   * Lance tout le pipeline ML :
   *  - feature engineering
   *  - train / test split
   *  - entraînement
   *  - évaluation
   *  - prédiction
   */
  def run(df: DataFrame): (PipelineModel, DataFrame) = {

    // =========================
    // 1. Feature engineering
    // =========================
    val featureCols = Array(
      "cases",
      "active",
      "critical",
      "tests",
      "population",
      "casesPerOneMillion",
      "deathsPerOneMillion"
    )

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    val preparedDf = assembler.transform(df)
      .select("features", "deaths")
      .withColumnRenamed("deaths", "label")

    // =========================
    // 2. Split train / test
    // =========================
    val Array(train, test) =
      preparedDf.randomSplit(Array(0.8, 0.2), seed = 42)

    // =========================
    // 3. Modèle
    // =========================
    val rf = new RandomForestRegressor()
      .setNumTrees(100)
      .setMaxDepth(10)
      .setFeaturesCol("features")
      .setLabelCol("label")

    // Pipeline (bonne pratique MLOps)
    val pipeline = new Pipeline()
      .setStages(Array(rf))

    // =========================
    // 4. Entraînement
    // =========================
    val model = pipeline.fit(train)

    // =========================
    // 5. Prédiction
    // =========================
    val predictions = model.transform(test)

    // =========================
    // 6. Évaluation
    // =========================
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("label")
      .setPredictionCol("prediction")

    val rmse = evaluator.evaluate(predictions)
    println(s"[ML] RMSE = $rmse")

    (model, predictions)
  }
}

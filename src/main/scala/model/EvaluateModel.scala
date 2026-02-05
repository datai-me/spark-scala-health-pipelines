package model

import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.evaluation.RegressionEvaluator

object EvaluateModel {

  def rmse(predictions: DataFrame): Double = {

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("label")
      .setPredictionCol("prediction")

    evaluator.evaluate(predictions)
  }

}

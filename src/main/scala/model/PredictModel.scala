package model

import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.PipelineModel

object PredictModel {

  def predict(model: PipelineModel, df: DataFrame): DataFrame = {
    model.transform(df)
  }

}

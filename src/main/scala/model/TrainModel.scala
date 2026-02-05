package model

import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.regression.RandomForestRegressionModel

object TrainModel {

  def train(trainDf: DataFrame): RandomForestRegressionModel = {

  val rf = new RandomForestRegressor()
    .setNumTrees(100)
    .setMaxDepth(10)
    .setFeaturesCol("features")
    .setLabelCol("label")

  rf.fit(trainDf)
}

}

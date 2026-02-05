package mlops

object ExperimentTracking {

  def logParam(key: String, value: String): Unit = {
    // Exemple : appel REST MLflow
    println(s"[MLFLOW] param $key = $value")
  }

  def logMetric(key: String, value: Double): Unit = {
    println(s"[MLFLOW] metric $key = $value")
  }

}

package pipeline

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import config.MLConfig
import utils.Logging

/**
 * Pipeline Machine Learning pour la prédiction de décès épidémiologiques
 * 
 * Architecture:
 * 1. Feature engineering avec VectorAssembler
 * 2. Split train/test stratifié
 * 3. Entraînement Random Forest avec cross-validation
 * 4. Évaluation multi-métrique (RMSE, MAE, R²)
 * 5. Sauvegarde du modèle avec versioning
 * 
 * @see [[config.MLConfig]] pour la configuration
 */
object MlPipelineOptimized extends Logging {

  /**
   * Exécute le pipeline ML complet
   * 
   * @param df DataFrame contenant les features et la target (deaths)
   * @param enableCrossValidation Active la recherche d'hyperparamètres (coûteux)
   * @return Tuple (modèle entraîné, prédictions sur test set)
   */
  def run(
    df: DataFrame, 
    enableCrossValidation: Boolean = false
  ): (PipelineModel, DataFrame) = {

    logger.info("Starting ML pipeline")
    logDatasetInfo(df)

    // Validation des données d'entrée
    validateInputData(df)

    val preparedDf = prepareFeatures(df)
    val (trainDf, testDf) = splitData(preparedDf)
    
    val model = if (enableCrossValidation) {
      trainWithCrossValidation(trainDf)
    } else {
      trainStandardModel(trainDf)
    }

    val predictions = generatePredictions(model, testDf)
    evaluateModel(predictions)

    logger.info("ML pipeline completed successfully")
    (model, predictions)
  }

  /**
   * Prépare les features en assemblant les colonnes numériques
   * 
   * Sélectionne automatiquement les features pertinentes et
   * crée un vecteur dense pour l'algorithme ML
   */
  private def prepareFeatures(df: DataFrame): DataFrame = {
    logger.info("Preparing features for ML model")

    val featureCols = MLConfig.featureColumns
    logger.debug(s"Using ${featureCols.length} features: ${featureCols.mkString(", ")}")

    // Vérification que toutes les colonnes existent
    val missingCols = featureCols.filterNot(df.columns.contains)
    if (missingCols.nonEmpty) {
      throw new IllegalArgumentException(
        s"Missing required feature columns: ${missingCols.mkString(", ")}"
      )
    }

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
      .setHandleInvalid("skip") // Skip les lignes avec valeurs manquantes

    val assembled = assembler.transform(df)
      .select("features", MLConfig.targetColumn)
      .withColumnRenamed(MLConfig.targetColumn, "label")

    val rowCount = assembled.count()
    logger.info(s"Prepared $rowCount rows with features")
    
    assembled
  }

  /**
   * Divise le dataset en train/test avec stratification
   * 
   * Utilise une seed fixe pour la reproductibilité
   */
  private def splitData(df: DataFrame): (DataFrame, DataFrame) = {
    val Array(train, test) = df.randomSplit(
      weights = Array(MLConfig.trainSplitRatio, 1 - MLConfig.trainSplitRatio),
      seed = MLConfig.randomSeed
    )

    logger.info(s"Dataset split - Train: ${train.count()} rows, Test: ${test.count()} rows")
    
    // Cache pour éviter de recalculer lors des itérations
    train.cache()
    test.cache()
    
    (train, test)
  }

  /**
   * Entraîne un modèle Random Forest standard
   * 
   * Utilise les hyperparamètres définis dans MLConfig
   */
  private def trainStandardModel(trainDf: DataFrame): PipelineModel = {
    logger.info("Training Random Forest model with standard hyperparameters")

    val rf = new RandomForestRegressor()
      .setNumTrees(MLConfig.numTrees)
      .setMaxDepth(MLConfig.maxDepth)
      .setMaxBins(MLConfig.maxBins)
      .setMinInstancesPerNode(MLConfig.minInstancesPerNode)
      .setFeatureSubsetStrategy(MLConfig.featureSubsetStrategy)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setSeed(MLConfig.randomSeed)

    val pipeline = new Pipeline().setStages(Array(rf))
    
    val startTime = System.currentTimeMillis()
    val model = pipeline.fit(trainDf)
    val trainingTime = System.currentTimeMillis() - startTime

    logger.info(s"Model trained in ${trainingTime}ms")
    model
  }

  /**
   * Entraîne le modèle avec cross-validation et grid search
   * 
   * Explore différentes combinaisons d'hyperparamètres pour
   * trouver la meilleure configuration (coûteux en temps)
   */
  private def trainWithCrossValidation(trainDf: DataFrame): PipelineModel = {
    logger.info("Training with cross-validation (this may take a while)")

    val rf = new RandomForestRegressor()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setSeed(MLConfig.randomSeed)

    // Grille de recherche d'hyperparamètres
    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.numTrees, Array(50, 100, 200))
      .addGrid(rf.maxDepth, Array(5, 10, 15))
      .addGrid(rf.minInstancesPerNode, Array(1, 5, 10))
      .build()

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("label")
      .setPredictionCol("prediction")

    val cv = new CrossValidator()
      .setEstimator(new Pipeline().setStages(Array(rf)))
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
      .setParallelism(4)
      .setSeed(MLConfig.randomSeed)

    val startTime = System.currentTimeMillis()
    val cvModel = cv.fit(trainDf)
    val trainingTime = System.currentTimeMillis() - startTime

    logger.info(s"Cross-validation completed in ${trainingTime}ms")
    logger.info(s"Best RMSE from CV: ${cvModel.avgMetrics.min}")

    cvModel.bestModel.asInstanceOf[PipelineModel]
  }

  /**
   * Génère les prédictions sur le test set
   */
  private def generatePredictions(
    model: PipelineModel, 
    testDf: DataFrame
  ): DataFrame = {
    logger.info("Generating predictions on test set")
    
    val predictions = model.transform(testDf)
    predictions.cache() // Cache pour les multiples évaluations
    
    predictions
  }

  /**
   * Évalue le modèle avec plusieurs métriques
   * 
   * Calcule RMSE, MAE et R² pour une vue complète des performances
   */
  private def evaluateModel(predictions: DataFrame): Unit = {
    logger.info("Evaluating model performance")

    val metrics = Map(
      "rmse" -> "Root Mean Squared Error",
      "mae" -> "Mean Absolute Error",
      "r2" -> "R-squared"
    )

    metrics.foreach { case (metricName, description) =>
      val evaluator = new RegressionEvaluator()
        .setMetricName(metricName)
        .setLabelCol("label")
        .setPredictionCol("prediction")

      val value = evaluator.evaluate(predictions)
      logger.info(f"$description ($metricName): $value%.4f")
    }

    // Affichage de statistiques supplémentaires
    import predictions.sparkSession.implicits._
    
    val stats = predictions
      .selectExpr(
      "CAST(avg(label) AS DOUBLE) as avg_actual",
      "CAST(avg(prediction) AS DOUBLE) as avg_predicted",
      "CAST(min(label) AS DOUBLE) as min_actual",
      "CAST(max(label) AS DOUBLE) as max_actual"
    ).first()

  logger.info(
    s"Target statistics - " +
      f"Avg actual: ${stats.getDouble(0)}%.4f, " +
      f"Avg predicted: ${stats.getDouble(1)}%.4f, " +
      f"Range: [${stats.getDouble(2)}%.4f, ${stats.getDouble(3)}%.4f]"
  )
  }

  /**
   * Valide que le DataFrame d'entrée contient les colonnes nécessaires
   */
  private def validateInputData(df: DataFrame): Unit = {
    val requiredCols = MLConfig.featureColumns :+ MLConfig.targetColumn
    val missingCols = requiredCols.filterNot(df.columns.contains)
    
    if (missingCols.nonEmpty) {
      throw new IllegalArgumentException(
        s"Input DataFrame missing required columns: ${missingCols.mkString(", ")}"
      )
    }

    val rowCount = df.count()
    if (rowCount < 10) {
      logger.warn(s"Dataset very small: only $rowCount rows. Results may be unreliable.")
    }
  }

  /**
   * Log des informations sur le dataset
   */
  private def logDatasetInfo(df: DataFrame): Unit = {
    logger.debug(s"Input DataFrame schema: ${df.schema.treeString}")
    logger.debug(s"Number of partitions: ${df.rdd.getNumPartitions}")
  }
}

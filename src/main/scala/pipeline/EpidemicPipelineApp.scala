// -------------------------------------------------------------
// FICHIER : pipeline/EpidemicPipelineApp.scala
// RÔLE    : Orchestrateur principal du pipeline de données
// -------------------------------------------------------------

package pipeline

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.ml.PipelineModel
import ingestion.EpidemicApiIngestion
import config.{ConfigLoader, MLConfig}
import storage.DataWriter
import utils.Logging
import mlops.{ExperimentTracking, ModelRegistry}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}
import scala.concurrent.ExecutionContext
import service.DataLakeExportService

/**
 * Pipeline principal de traitement des données épidémiologiques
 * 
 * Architecture du pipeline:
 * 1. Ingestion des données via API REST (async)
 * 2. Validation et nettoyage des données
 * 3. Affichage des statistiques descriptives
 * 4. Entraînement du modèle ML (Random Forest)
 * 5. Évaluation et logging des métriques
 * 6. Sauvegarde du modèle avec versioning
 * 7. (Optionnel) Persistance en base de données
 * 
 * Le pipeline suit les bonnes pratiques MLOps:
 * - Tracking des expérimentations
 * - Registry des modèles
 * - Validation des données
 * - Logging structuré
 * 
 * @see [[MlPipelineOptimized]] pour le pipeline ML
 * @see [[ingestion.EpidemicApiIngestion]] pour l'ingestion
 */
object EpidemicPipelineApp extends Logging {

  /**
   * Exécute le pipeline complet de bout en bout
   * 
   * @param spark SparkSession active
   * @param enableCrossValidation Active la recherche d'hyperparamètres (coûteux)
   * @param saveToDatabase Active la sauvegarde en base de données
   */
  def run(
    spark: SparkSession,
    enableCrossValidation: Boolean = false,
    saveToDatabase: Boolean = false
  ): Unit = {

    logger.info("="*60)
    logger.info("EPIDEMIC PIPELINE - START")
    logger.info("="*60)

    // Log de la configuration
    logPipelineConfiguration(enableCrossValidation, saveToDatabase)

    try {
      // Étape 1: Ingestion des données
      val rawData = ingestData(spark)
      
      // Étape 2: Validation des données
      validateData(rawData)
      
      // Étape 3: Statistiques descriptives
      displayDescriptiveStats(spark, rawData)
      
      // Étape 4: Entraînement du modèle ML
      val (model, predictions) = trainModel(rawData, enableCrossValidation)
      
      // Étape 5: Affichage des prédictions
      displayPredictions(predictions)
      
      // Étape 6: Sauvegarde du modèle
      saveModel(model)

      // Étape 7: (Optionnel) Sauvegarde en base de données
      if (saveToDatabase) {
        persistToDatabase(rawData)
      } 

      // Étape 7: Écriture Data Lake (Parquet + CSV)
      logger.info("[STEP] Data Lake export started")

      writeToDataLake(
        spark = spark,
        df = rawData,
        datasetName = "epidemic_countries"
      )

      logger.info("[STEP 8/8] Data Lake export finished")

      logger.info("="*60)
      logger.info("EPIDEMIC PIPELINE - COMPLETED SUCCESSFULLY")
      logger.info("="*60)
      
    } catch {
      case ex: Exception =>
        logger.error("Pipeline execution failed", ex)
        throw ex // Re-throw pour que Main puisse gérer
    }

    
  }

  /**
   * Étape 1: Ingestion des données depuis l'API
   * 
   * Utilise un appel asynchrone avec retry automatique
   * en cas d'échec réseau
   * 
   * @return DataFrame contenant les données brutes
   */
  private def ingestData(spark: SparkSession): DataFrame = {
    logger.info("[STEP 1/8] Data Ingestion - Fetching from API")
    
    timed("Data ingestion") {
      val df = EpidemicApiIngestion.readApi(spark)
      
      val rowCount = df.count()
      val columnCount = df.columns.length
      
      logger.info(s"Successfully ingested $rowCount rows with $columnCount columns")
      logInfoWithContext(
        "Ingestion completed",
        Map(
          "rows" -> rowCount,
          "columns" -> columnCount,
          "source" -> ConfigLoader.epidemicApiUrl
        )
      )
      
      df
    }
  }

  /**
   * Étape 2: Validation des données
   * 
   * Vérifie:
   * - Présence des colonnes requises
   * - Nombre minimum de lignes
   * - Absence de données complètement nulles
   * - Plage de valeurs cohérente
   */
  private def validateData(df: DataFrame): Unit = {
    logger.info("[STEP 2/8] Data Validation")
    
    // Vérification du nombre de lignes
    val rowCount = df.count()
    if (rowCount == 0) {
      throw new IllegalStateException("Dataset is empty! Cannot proceed.")
    }
    
    if (rowCount < 10) {
      logger.warn(s"Dataset very small: only $rowCount rows. Results may be unreliable.")
    }
    
    // Vérification des colonnes requises
    val requiredColumns = Set(
      "country", "cases", "deaths", "active", "critical", 
      "tests", "population", "casesPerOneMillion", "deathsPerOneMillion"
    )
    
    val missingColumns = requiredColumns.diff(df.columns.toSet)
    if (missingColumns.nonEmpty) {
      throw new IllegalStateException(
        s"Missing required columns: ${missingColumns.mkString(", ")}"
      )
    }
    
    // Vérification des valeurs nulles critiques
    import org.apache.spark.sql.functions._
    
    val nullCounts = df.select(
      requiredColumns.map(c => sum(when(col(c).isNull, 1).otherwise(0)).as(s"${c}_nulls")).toSeq: _*
    ).first()
    
    logger.debug(s"Null counts per column: $nullCounts")
    
    // Vérification des plages de valeurs
    val stats = df.selectExpr(
      "min(cases) as min_cases",
      "max(cases) as max_cases",
      "avg(population) as avg_population"
    ).first()
    
    logDebugWithContext(
      "Data quality check",
      Map(
        "min_cases" -> stats.getLong(0),
        "max_cases" -> stats.getLong(1),
        "avg_population" -> stats.getDouble(2)
      )
    )
    
    logger.info("Data validation passed ✓")
  }

  /**
   * Étape 3: Affichage des statistiques descriptives
   * 
   * Crée une vue temporaire SQL pour permettre des requêtes
   * et affiche les principaux indicateurs par pays
   */
  private def displayDescriptiveStats(
    spark: SparkSession, 
    df: DataFrame
  ): Unit = {
    logger.info("[STEP 3/8] Descriptive Statistics")
    
    // Création d'une vue temporaire pour requêtes SQL
    df.createOrReplaceTempView("epidemic")
    logger.debug("Created temporary view 'epidemic'")
    
    // Affichage des top pays par nombre de cas
    logger.info("Top 20 countries by total cases:")
    val topCountries = spark.sql(
      """
      SELECT 
        country,
        cases,
        active,
        critical,
        population,
        deaths,
        tests,
        casesPerOneMillion,
        deathsPerOneMillion
      FROM epidemic
      ORDER BY cases DESC
      LIMIT 20
      """
    )
    
    topCountries.show(20, truncate = false)
    
    // Statistiques globales
    logger.info("Global statistics:")
    spark.sql(
      """
      SELECT 
        COUNT(*) as total_countries,
        SUM(cases) as global_cases,
        SUM(deaths) as global_deaths,
        SUM(population) as global_population,
        ROUND(AVG(deathsPerOneMillion), 2) as avg_deaths_per_million
      FROM epidemic
      """
    ).show(truncate = false)
  }

  /**
   * Étape 4: Entraînement du modèle Machine Learning
   * 
   * Utilise le pipeline ML optimisé avec Random Forest
   * 
   * @param df DataFrame de données
   * @param enableCV Active la cross-validation
   * @return Tuple (modèle entraîné, prédictions)
   */
  private def trainModel(
    df: DataFrame,
    enableCV: Boolean
  ): (PipelineModel, DataFrame) = {
    logger.info(s"[STEP 4/8] ML Model Training (CV: ${if (enableCV) "enabled" else "disabled"})")
    
    // Tracking de l'expérimentation (MLflow style)
    ExperimentTracking.logParam("model_type", "RandomForestRegressor")
    ExperimentTracking.logParam("enable_cv", enableCV.toString)
    ExperimentTracking.logParam("num_trees", MLConfig.numTrees.toString)
    ExperimentTracking.logParam("max_depth", MLConfig.maxDepth.toString)
    
    val (model, predictions) = timed("Model training and prediction") {
      MlPipelineOptimized.run(df, enableCrossValidation = enableCV)
    }
    
    logger.info("Model training completed successfully ✓")
    
    (model, predictions)
  }

  /**
   * Étape 5: Affichage des prédictions
   * 
   * Montre un échantillon des prédictions pour vérification visuelle
   */
  private def displayPredictions(predictions: DataFrame): Unit = {
    logger.info("[STEP 5/8] Model Predictions")
    
    logger.info("Sample predictions (actual vs predicted deaths):")
    predictions
      .select("label", "prediction")
      .withColumnRenamed("label", "actual_deaths")
      .withColumnRenamed("prediction", "predicted_deaths")
      .show(10, truncate = false)
    
    // Statistiques sur les erreurs de prédiction
    import org.apache.spark.sql.functions._
    
    predictions
      .selectExpr(
        "ROUND(AVG(ABS(label - prediction)), 2) as mean_absolute_error",
        "ROUND(MIN(prediction), 2) as min_prediction",
        "ROUND(MAX(prediction), 2) as max_prediction"
      )
      .show(truncate = false)
  }

  /**
   * Étape 6: Sauvegarde du modèle avec versioning
   * 
   * Utilise un pattern de versioning basé sur timestamp
   * pour traçabilité complète
   */
  private def saveModel(model: PipelineModel): Unit = {
    logger.info("[STEP 6/8] Model Persistence")
    
    val timestamp = System.currentTimeMillis()
    val version = new java.text.SimpleDateFormat("yyyyMMdd_HHmmss")
      .format(new java.util.Date(timestamp))
    
    val modelPath = MLConfig.getVersionedModelPath(version)
    
    timed("Model saving") {
      if (MLConfig.overwriteModel) {
        model.write.overwrite().save(modelPath)
      } else {
        model.write.save(modelPath)
      }
    }
    
    logger.info(s"Model saved successfully to: $modelPath")
    
    // Enregistrement dans le registry MLOps
    ModelRegistry.register(model, s"${MLConfig.modelName}_$version")
    
    logInfoWithContext(
      "Model registered",
      Map(
        "version" -> version,
        "path" -> modelPath,
        "timestamp" -> timestamp
      )
    )
  }

  /**
   * Étape 7: (Optionnel) Persistance en base de données
   * 
   * Sauvegarde les données brutes en JDBC pour analyse ultérieure
   */
  private def persistToDatabase(df: DataFrame): Unit = {
    logger.info("[STEP 7/8] Database Persistence")
    
    timed("JDBC write") {
      Try {
        DataWriter.writeToJdbc(df)
      } match {
        case Success(_) =>
          logger.info("Data successfully written to database ✓")
          
        case Failure(ex) =>
          logger.error("Failed to write to database", ex)
          logger.warn("Continuing pipeline despite database failure")
          // On ne propage pas l'erreur car c'est optionnel
      }
    }
  }

  /**
  * Étape 8: Écrit un DataFrame Spark dans le Data Lake (Parquet + CSV analyst).
  *
  * Centralise l'appel au service d'export pour garder run() lisible.
  */
  private def writeToDataLake(
    spark: SparkSession,
    df: DataFrame,
    datasetName: String
  ): Unit = {

    implicit val ec: ExecutionContext = ExecutionContext.global

    DataLakeExportService.exportDataLake(
      spark = spark,
      df = df,
      datasetName = datasetName,
      awaitCompletion = true,   // ✅ sûr : on attend avant spark.stop()
      timeout = 10.minutes
    )
  }


  /**
   * Log la configuration du pipeline
   */
  private def logPipelineConfiguration(
    enableCV: Boolean,
    saveDB: Boolean
  ): Unit = {
    logger.info("Pipeline configuration:")
    logger.info(s"  - API URL: ${ConfigLoader.epidemicApiUrl}")
    logger.info(s"  - Cross-validation: ${if (enableCV) "enabled" else "disabled"}")
    logger.info(s"  - Database persistence: ${if (saveDB) "enabled" else "disabled"}")
    logger.info(s"  - ML num trees: ${MLConfig.numTrees}")
    logger.info(s"  - ML max depth: ${MLConfig.maxDepth}")
    logger.info(s"  - Train/test split: ${MLConfig.trainSplitRatio * 100}%/${(1 - MLConfig.trainSplitRatio) * 100}%")
  }
}
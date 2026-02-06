package config

/**
 * Configuration centralisée pour le pipeline Machine Learning
 * 
 * Tous les hyperparamètres et settings sont externalisés via
 * variables d'environnement pour faciliter le tuning et les
 * déploiements en environnements différents (dev/staging/prod)
 * 
 * Valeurs par défaut optimisées pour un dataset épidémiologique
 * de taille moyenne (~200 pays)
 */
object MLConfig {

  // ============================================================
  // FEATURE ENGINEERING
  // ============================================================

  /**
   * Colonnes utilisées comme features pour le modèle
   * Correspond aux indicateurs épidémiologiques pertinents
   */
  val featureColumns: Array[String] = sys.env
    .getOrElse("ML_FEATURE_COLUMNS", 
      "cases,active,critical,tests,population,casesPerOneMillion,deathsPerOneMillion"
    )
    .split(",")
    .map(_.trim)

  /**
   * Colonne cible à prédire (variable dépendante)
   */
  val targetColumn: String = sys.env.getOrElse("ML_TARGET_COLUMN", "deaths")

  // ============================================================
  // TRAIN/TEST SPLIT
  // ============================================================

  /**
   * Ratio de données pour l'entraînement (0.0 à 1.0)
   * Default: 80% train, 20% test
   */
  val trainSplitRatio: Double = sys.env
    .getOrElse("ML_TRAIN_RATIO", "0.8")
    .toDouble

  /**
   * Seed pour la reproductibilité des splits
   */
  val randomSeed: Long = sys.env
    .getOrElse("ML_RANDOM_SEED", "42")
    .toLong

  // ============================================================
  // RANDOM FOREST HYPERPARAMETERS
  // ============================================================

  /**
   * Nombre d'arbres dans la forêt
   * Plus élevé = meilleure précision mais plus lent
   * Recommandation: 100-200 pour production
   */
  val numTrees: Int = sys.env
    .getOrElse("ML_NUM_TREES", "100")
    .toInt

  /**
   * Profondeur maximale de chaque arbre
   * Contrôle l'overfitting (profondeur élevée = risque d'overfitting)
   * Recommandation: 10-15 pour éviter l'overfitting
   */
  val maxDepth: Int = sys.env
    .getOrElse("ML_MAX_DEPTH", "10")
    .toInt

  /**
   * Nombre maximum de bins pour discrétiser les features continues
   * Plus élevé = meilleure granularité mais plus de mémoire
   * Doit être >= au nombre de catégories dans les features catégorielles
   */
  val maxBins: Int = sys.env
    .getOrElse("ML_MAX_BINS", "32")
    .toInt

  /**
   * Nombre minimum d'instances requises dans un nœud pour le diviser
   * Prévient l'overfitting sur des cas trop spécifiques
   */
  val minInstancesPerNode: Int = sys.env
    .getOrElse("ML_MIN_INSTANCES_PER_NODE", "1")
    .toInt

  /**
   * Stratégie de sélection des features à chaque split
   * Options: "auto", "all", "sqrt", "log2", "onethird"
   * "auto" = sqrt(n) pour regression, équilibré entre vitesse et précision
   */
  val featureSubsetStrategy: String = sys.env
    .getOrElse("ML_FEATURE_SUBSET_STRATEGY", "auto")

  /**
   * Impureté pour les critères de split
   * Pour régression: "variance" (seule option)
   */
  val impurity: String = "variance"

  // ============================================================
  // MODEL EVALUATION
  // ============================================================

  /**
   * Métrique principale d'évaluation
   * Options: "rmse", "mae", "r2", "mse"
   */
  val primaryMetric: String = sys.env
    .getOrElse("ML_PRIMARY_METRIC", "rmse")

  /**
   * Seuil d'acceptation pour RMSE (erreur acceptable)
   * Si RMSE > seuil, le modèle est rejeté
   */
  val rmseThreshold: Double = sys.env
    .getOrElse("ML_RMSE_THRESHOLD", "10000.0")
    .toDouble

  // ============================================================
  // MODEL PERSISTENCE
  // ============================================================

  /**
   * Chemin de base pour sauvegarder les modèles
   */
  val modelBasePath: String = sys.env
    .getOrElse("ML_MODEL_BASE_PATH", "/models/epidemic")

  /**
   * Nom du modèle pour versioning
   */
  val modelName: String = sys.env
    .getOrElse("ML_MODEL_NAME", "random_forest_deaths_predictor")

  /**
   * Activer l'écrasement des modèles existants
   */
  val overwriteModel: Boolean = sys.env
    .getOrElse("ML_OVERWRITE_MODEL", "true")
    .toBoolean

  // ============================================================
  // CROSS-VALIDATION (optionnel, coûteux)
  // ============================================================

  /**
   * Activer la cross-validation avec grid search
   * WARNING: peut prendre beaucoup de temps
   */
  val enableCrossValidation: Boolean = sys.env
    .getOrElse("ML_ENABLE_CV", "false")
    .toBoolean

  /**
   * Nombre de folds pour la cross-validation
   */
  val cvNumFolds: Int = sys.env
    .getOrElse("ML_CV_NUM_FOLDS", "3")
    .toInt

  /**
   * Niveau de parallélisme pour la cross-validation
   * Nombre de combinaisons d'hyperparamètres à tester en parallèle
   */
  val cvParallelism: Int = sys.env
    .getOrElse("ML_CV_PARALLELISM", "4")
    .toInt

  // ============================================================
  // HELPER METHODS
  // ============================================================

  /**
   * Retourne un chemin complet pour sauvegarder un modèle versionné
   */
  def getVersionedModelPath(version: String): String = {
    s"$modelBasePath/${modelName}_v$version"
  }

  /**
   * Affiche la configuration actuelle (utile pour le debugging)
   */
  def printConfig(): Unit = {
    println(
      s"""
         |========================================
         |  ML Pipeline Configuration
         |========================================
         |Features: ${featureColumns.mkString(", ")}
         |Target: $targetColumn
         |Train/Test split: ${trainSplitRatio * 100}% / ${(1 - trainSplitRatio) * 100}%
         |
         |Random Forest params:
         |  - Num trees: $numTrees
         |  - Max depth: $maxDepth
         |  - Max bins: $maxBins
         |  - Min instances per node: $minInstancesPerNode
         |  - Feature subset strategy: $featureSubsetStrategy
         |
         |Evaluation:
         |  - Primary metric: $primaryMetric
         |  - RMSE threshold: $rmseThreshold
         |
         |Model persistence:
         |  - Path: $modelBasePath
         |  - Name: $modelName
         |  - Overwrite: $overwriteModel
         |
         |Cross-validation: ${if (enableCrossValidation) "ENABLED" else "DISABLED"}
         |========================================
         |""".stripMargin
    )
  }
}

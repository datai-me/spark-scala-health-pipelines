package pipeline

import org.apache.spark.ml.linalg.Vector
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import config.MLConfig

/**
 * Tests unitaires pour le pipeline ML
 * 
 * Framework: ScalaTest
 * Dépendances requises (build.sbt):
 * {{{
 * libraryDependencies ++= Seq(
 *   "org.scalatest" %% "scalatest" % "3.2.19" % Test,
 *   "org.apache.spark" %% "spark-sql" % "3.5.0" % Test,
 *   "org.apache.spark" %% "spark-mllib" % "3.5.0" % Test
 * )
 * }}}
 */
class MlPipelineOptimizedSpec 
  extends AnyFlatSpec 
  with Matchers 
  with BeforeAndAfterAll 
  with SparkTestHelper {

  // ============================================================
  // SETUP & TEARDOWN
  // ============================================================

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Configuration supplémentaire si nécessaire
  }

  override def afterAll(): Unit = {
    stopSpark()
    super.afterAll()
  }

  // ============================================================
  // TESTS - HAPPY PATH
  // ============================================================

  behavior of "MlPipelineOptimized.run"

  it should "successfully train a model with valid epidemic data" in {
    val testDf = createValidEpidemicDataFrame(100)
    
    val (model, predictions) = MlPipelineOptimized.run(testDf)
    
    model should not be null
    predictions.count() should be > 0L
    predictions.columns should contain allOf ("label", "prediction", "features")
  }

  it should "produce predictions within reasonable bounds" in {
    val testDf = createValidEpidemicDataFrame(100)
    
    val (_, predictions) = MlPipelineOptimized.run(testDf)
    
    // Les prédictions doivent être positives (nombre de décès)
    val negativePredictions = predictions
      .filter(col("prediction") < 0)
      .count()
    
    negativePredictions shouldBe 0
  }

  it should "cache intermediate DataFrames to optimize performance" in {
    val testDf = createValidEpidemicDataFrame(100)
    
    val (model, predictions) = MlPipelineOptimized.run(testDf)
    
    // Vérifier que les DataFrames sont bien cachés
    predictions.storageLevel.useMemory shouldBe true
  }

  // ============================================================
  // TESTS - EDGE CASES
  // ============================================================

  it should "handle small datasets gracefully" in {
    val testDf = createValidEpidemicDataFrame(10)
    
    // Ne devrait pas crasher même avec peu de données
    noException should be thrownBy {
      MlPipelineOptimized.run(testDf)
    }
  }

  it should "reject DataFrames with missing required columns" in {
    val incompleteDf = spark.createDataFrame(
      Seq((1000L, 50L, 2.5))
    ).toDF("cases", "active", "deaths") // Manque critical, tests, etc.
    
    assertThrows[IllegalArgumentException] {
      MlPipelineOptimized.run(incompleteDf)
    }
  }

  it should "handle DataFrames with null values appropriately" in {
    val dfWithNulls = createEpidemicDataFrameWithNulls(100, nullRatio = 0.2)
    
    val (_, predictions) = MlPipelineOptimized.run(dfWithNulls)
    
    // Les lignes avec nulls doivent être skipped par VectorAssembler
    predictions.count() should be < 100L
  }

  // ============================================================
  // TESTS - MODEL QUALITY
  // ============================================================

  it should "achieve reasonable RMSE on test data" in {
    val testDf = createValidEpidemicDataFrame(200)
    
    val (_, predictions) = MlPipelineOptimized.run(testDf)
    
    val evaluator = new org.apache.spark.ml.evaluation.RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("label")
      .setPredictionCol("prediction")
    
    val rmse = evaluator.evaluate(predictions)
    
    // RMSE devrait être < au seuil configuré
    rmse should be < MLConfig.rmseThreshold
  }

  it should "show correlation between actual and predicted values" in {
    val testDf = createValidEpidemicDataFrame(200)
    
    val (_, predictions) = MlPipelineOptimized.run(testDf)
    
    import spark.implicits._
    
    val correlation = predictions
      .stat
      .corr("label", "prediction")
    
    // Corrélation devrait être positive et significative
    correlation should be > 0.5
  }

  // ============================================================
  // TESTS - CROSS-VALIDATION
  // ============================================================

  it should "support cross-validation when enabled" in {
    val testDf = createValidEpidemicDataFrame(200)
    
    // CV est coûteux, on teste avec des paramètres réduits
    val (model, _) = MlPipelineOptimized.run(testDf, enableCrossValidation = true)
    
    model should not be null
    // Le modèle devrait être le meilleur modèle trouvé par CV
  }

  // ============================================================
  // TESTS - PERFORMANCE
  // ============================================================

  it should "complete training in reasonable time" in {
    val testDf = createValidEpidemicDataFrame(100)
    
    val startTime = System.currentTimeMillis()
    MlPipelineOptimized.run(testDf)
    val duration = System.currentTimeMillis() - startTime
    
    // Training ne devrait pas prendre plus de 30 secondes sur petit dataset
    duration should be < 30000L
  }

  // ============================================================
  // TESTS - FEATURE ENGINEERING
  // ============================================================

  behavior of "Feature preparation"

  it should "create feature vectors with correct dimensions" in {
    val testDf = createValidEpidemicDataFrame(50)
    
    val (_, predictions) = MlPipelineOptimized.run(testDf)
    
    import predictions.sparkSession.implicits._
    
    val featureVectorSize: Int =
  predictions.select("features").head().getAs[Vector]("features").size
    
    featureVectorSize shouldBe MLConfig.featureColumns.length
  }

  // ============================================================
  // TESTS - ERROR HANDLING
  // ============================================================

  it should "provide meaningful error messages for invalid input" in {
    val emptyDf = spark.emptyDataFrame
    
    val exception = intercept[IllegalArgumentException] {
      MlPipelineOptimized.run(emptyDf)
    }
    
    exception.getMessage should include("missing required columns")
  }
}

/**
 * Trait helper pour créer des SparkSessions de test
 * et des DataFrames de données épidémiologiques
 */
trait SparkTestHelper {
  
  private var _spark: SparkSession = _

  /**
   * SparkSession partagée pour tous les tests
   */
  implicit lazy val spark: SparkSession = {
    if (_spark == null) {
      _spark = SparkSession.builder()
        .appName("MlPipelineTest")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .getOrCreate()
      
      _spark.sparkContext.setLogLevel("WARN")
    }
    _spark
  }

  /**
   * Arrête proprement la SparkSession
   */
  def stopSpark(): Unit = {
    if (_spark != null) {
      _spark.stop()
      _spark = null
    }
  }

  /**
   * Crée un DataFrame de test avec des données épidémiologiques valides
   * 
   * @param numRows Nombre de pays à générer
   * @return DataFrame avec toutes les colonnes requises
   */
  def createValidEpidemicDataFrame(numRows: Int): DataFrame = {
    import spark.implicits._
    
    val data = (1 to numRows).map { i =>
      val population = 1000000L + (i * 10000)
      val cases = (population * 0.05).toLong
      val active = (cases * 0.3).toLong
      val critical = (active * 0.1).toLong
      val tests = (population * 0.2).toLong
      val deaths = (cases * 0.02).toLong
      val casesPerMillion = (cases * 1000000) / population
      val deathsPerMillion = (deaths * 1000000) / population
      
      (
        cases,
        active,
        critical,
        tests,
        population,
        casesPerMillion,
        deathsPerMillion,
        deaths
      )
    }
    
    data.toDF(
      "cases",
      "active", 
      "critical",
      "tests",
      "population",
      "casesPerOneMillion",
      "deathsPerOneMillion",
      "deaths"
    )
  }

  /**
   * Crée un DataFrame avec des valeurs nulles pour tester la robustesse
   */
  def createEpidemicDataFrameWithNulls(
    numRows: Int, 
    nullRatio: Double
  ): DataFrame = {
    import spark.implicits._
    
    val baseData = createValidEpidemicDataFrame(numRows)
    
    // Injecter aléatoirement des nulls
    val withNulls = baseData.withColumn(
      "cases",
      when(rand() < nullRatio, lit(null)).otherwise(col("cases"))
    ).withColumn(
      "active",
      when(rand() < nullRatio, lit(null)).otherwise(col("active"))
    )
    
    withNulls
  }

  /**
   * Crée un DataFrame minimal pour les tests négatifs
   */
  def createMinimalDataFrame(numRows: Int): DataFrame = {
    import spark.implicits._
    
    (1 to numRows).map(i => (i.toLong, i.toLong))
      .toDF("cases", "deaths")
  }
}

package utils

import org.apache.spark.sql.SparkSession
import scala.util.Properties

/**
 * Utilitaire pour afficher les versions de toutes les dépendances
 * de l'environnement d'exécution
 * 
 * Affiche:
 * - Versions Java (runtime + vendor)
 * - Version Scala
 * - Version Apache Spark + configuration
 * - Version Hadoop (bundled avec Spark)
 * - OS et architecture
 * - Mémoire disponible
 * 
 * Utile pour:
 * - Debugging de problèmes de compatibilité
 * - Documentation de l'environnement d'exécution
 * - Audits de sécurité (versions vulnérables)
 * - Rapport de support technique
 */
object VersionsInfo extends Logging {

  /**
   * Affiche toutes les informations de version dans un format structuré
   * 
   * @param spark SparkSession active pour récupérer les infos Spark/Hadoop
   */
  def printVersions(spark: SparkSession): Unit = {
    logger.info("Collecting environment information...")
    
    val separator = "=" * 70
    val subSeparator = "-" * 70
    
    println()
    println(separator)
    println(centerText("EPIDEMIC PIPELINE - ENVIRONMENT VERSIONS", 70))
    println(separator)
    println()
    
    // Section: Java
    printJavaInfo()
    println(subSeparator)
    
    // Section: Scala
    printScalaInfo()
    println(subSeparator)
    
    // Section: Apache Spark
    printSparkInfo(spark)
    println(subSeparator)
    
    // Section: Hadoop
    printHadoopInfo()
    println(subSeparator)
    
    // Section: Système d'exploitation
    printSystemInfo()
    println(subSeparator)
    
    // Section: Mémoire JVM
    printMemoryInfo()
    
    println(separator)
    println()
    
    logger.debug("Environment information displayed successfully")
  }

  /**
   * Affiche les informations Java
   */
  private def printJavaInfo(): Unit = {
    println("📦 JAVA")
    println(s"   Version              : ${System.getProperty("java.version")}")
    println(s"   Vendor               : ${System.getProperty("java.vendor")}")
    println(s"   Runtime version      : ${System.getProperty("java.runtime.version")}")
    println(s"   Home directory       : ${System.getProperty("java.home")}")
    
    val vmName = System.getProperty("java.vm.name")
    val vmVersion = System.getProperty("java.vm.version")
    println(s"   VM                   : $vmName $vmVersion")
  }

  /**
   * Affiche les informations Scala
   */
  private def printScalaInfo(): Unit = {
    println("📦 SCALA")
    println(s"   Version              : ${Properties.versionNumberString}")
    println(s"   Release              : ${Properties.releaseVersion.getOrElse("unknown")}")
    println(s"   Library version      : ${Properties.versionString}")
  }

  /**
   * Affiche les informations Apache Spark
   */
  private def printSparkInfo(spark: SparkSession): Unit = {
    println("⚡ APACHE SPARK")
    println(s"   Version              : ${spark.version}")
    println(s"   Master               : ${spark.sparkContext.master}")
    println(s"   Application ID       : ${spark.sparkContext.applicationId}")
    println(s"   Application Name     : ${spark.sparkContext.appName}")
    println(s"   Default parallelism  : ${spark.sparkContext.defaultParallelism}")
    
    // Configurations importantes
    val conf = spark.sparkContext.getConf
    
    val adaptiveEnabled = conf.getOption("spark.sql.adaptive.enabled")
      .getOrElse("false")
    println(s"   Adaptive QE          : $adaptiveEnabled")
    
    val serializer = conf.getOption("spark.serializer")
      .getOrElse("default")
      .split('.').last
    println(s"   Serializer           : $serializer")
    
    val shufflePartitions = conf.getOption("spark.sql.shuffle.partitions")
      .getOrElse("200")
    println(s"   Shuffle partitions   : $shufflePartitions")
  }

  /**
   * Affiche les informations Hadoop
   */
  private def printHadoopInfo(): Unit = {
    println("🐘 HADOOP")
    
    try {
      val hadoopVersion = org.apache.hadoop.util.VersionInfo.getVersion
      val hadoopCompiledBy = org.apache.hadoop.util.VersionInfo.getUser
      val hadoopDate = org.apache.hadoop.util.VersionInfo.getDate
      
      println(s"   Version              : $hadoopVersion")
      println(s"   Compiled by          : $hadoopCompiledBy")
      println(s"   Compiled date        : $hadoopDate")
      
    } catch {
      case ex: Exception =>
        println(s"   Version              : Unable to determine (${ex.getMessage})")
        logger.debug("Failed to get Hadoop version", ex)
    }
  }

  /**
   * Affiche les informations système
   */
  private def printSystemInfo(): Unit = {
    println("💻 SYSTEM")
    println(s"   OS Name              : ${System.getProperty("os.name")}")
    println(s"   OS Version           : ${System.getProperty("os.version")}")
    println(s"   OS Architecture      : ${System.getProperty("os.arch")}")
    println(s"   User                 : ${System.getProperty("user.name")}")
    println(s"   User directory       : ${System.getProperty("user.dir")}")
    
    val processors = Runtime.getRuntime.availableProcessors()
    println(s"   Available processors : $processors")
  }

  /**
   * Affiche les informations mémoire JVM
   */
  private def printMemoryInfo(): Unit = {
    val runtime = Runtime.getRuntime
    val mb = 1024 * 1024
    
    val maxMemory = runtime.maxMemory() / mb
    val totalMemory = runtime.totalMemory() / mb
    val freeMemory = runtime.freeMemory() / mb
    val usedMemory = totalMemory - freeMemory
    
    println("💾 MEMORY (JVM)")
    println(f"   Max heap             : $maxMemory%,d MB")
    println(f"   Total allocated      : $totalMemory%,d MB")
    println(f"   Used                 : $usedMemory%,d MB")
    println(f"   Free                 : $freeMemory%,d MB")
    
    val usagePercentage = (usedMemory.toDouble / maxMemory * 100).toInt
    println(f"   Usage                : $usagePercentage%d%%")
    
    if (usagePercentage > 80) {
      logger.warn(s"High memory usage detected: $usagePercentage%")
    }
  }

  /**
   * Centre un texte dans une largeur donnée
   */
  private def centerText(text: String, width: Int): String = {
    val padding = (width - text.length) / 2
    (" " * padding) + text
  }

  /**
   * Retourne toutes les informations sous forme de Map
   * (utile pour logging structuré ou export JSON)
   * 
   * @param spark SparkSession active
   * @return Map contenant toutes les versions
   */
  def getVersionsMap(spark: SparkSession): Map[String, String] = {
    Map(
      // Java
      "java.version" -> System.getProperty("java.version"),
      "java.vendor" -> System.getProperty("java.vendor"),
      "java.vm.name" -> System.getProperty("java.vm.name"),
      
      // Scala
      "scala.version" -> Properties.versionNumberString,
      
      // Spark
      "spark.version" -> spark.version,
      "spark.master" -> spark.sparkContext.master,
      "spark.app.id" -> spark.sparkContext.applicationId,
      
      // Hadoop
      "hadoop.version" -> org.apache.hadoop.util.VersionInfo.getVersion,
      
      // System
      "os.name" -> System.getProperty("os.name"),
      "os.version" -> System.getProperty("os.version"),
      "os.arch" -> System.getProperty("os.arch")
    )
  }

  /**
   * Vérifie si les versions minimum requises sont satisfaites
   * 
   * @param spark SparkSession active
   * @return true si toutes les versions sont compatibles
   */
  def checkMinimumVersions(spark: SparkSession): Boolean = {
    logger.info("Checking minimum required versions...")
    
    var allValid = true
    
    // Java 11+ requis
    val javaVersion = System.getProperty("java.version")
    val javaMajor = javaVersion.split('.').head.toInt
    if (javaMajor < 11) {
      logger.error(s"Java 11+ required, found: $javaVersion")
      allValid = false
    } else {
      logger.debug(s"Java version check passed: $javaVersion")
    }
    
    // Scala 2.12+ ou 2.13
    val scalaVersion = Properties.versionNumberString
    if (!scalaVersion.startsWith("2.12") && !scalaVersion.startsWith("2.13")) {
      logger.warn(s"Scala 2.12 or 2.13 recommended, found: $scalaVersion")
    }
    
    // Spark 3.0+
    val sparkVersion = spark.version
    val sparkMajor = sparkVersion.split('.').head.toInt
    if (sparkMajor < 3) {
      logger.error(s"Spark 3.0+ required, found: $sparkVersion")
      allValid = false
    } else {
      logger.debug(s"Spark version check passed: $sparkVersion")
    }
    
    if (allValid) {
      logger.info("All version requirements satisfied ✓")
    } else {
      logger.error("Some version requirements not met!")
    }
    
    allValid
  }
}
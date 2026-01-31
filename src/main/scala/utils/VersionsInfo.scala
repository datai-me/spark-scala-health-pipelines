package utils

import org.apache.spark.sql.SparkSession

object VersionsInfo {

  def printVersions(spark: SparkSession): Unit = {

    println("=================================================")
    println("      PIPELINE ENVIRONMENT VERSIONS")
    println("=================================================")

    // Java
    println(s"Java version        : ${System.getProperty("java.version")}")
    println(s"Java vendor         : ${System.getProperty("java.vendor")}")

    // Scala
    println(s"Scala version       : ${util.Properties.versionNumberString}")

    // Spark
    println(s"Spark version       : ${spark.version}")

    // Hadoop (bundled with Spark)
    val hadoopVersion =
      org.apache.hadoop.util.VersionInfo.getVersion
    println(s"Hadoop version      : $hadoopVersion")

    println("=================================================\n")
  }
}

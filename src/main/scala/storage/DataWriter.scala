package storage

import org.apache.spark.sql.DataFrame
import config.ConfigLoader

/**
 * Responsable uniquement de la persistance des données
 * (Single Responsibility Principle)
 */
object DataWriter {

  /**
   * Écrit un DataFrame dans une base via JDBC
   */
  def writeToJdbc(df: DataFrame): Unit = {

    df.write
      .format("jdbc")
      .option("url", ConfigLoader.jdbcUrl)
      .option("dbtable", ConfigLoader.jdbcTable)
      .option("user", ConfigLoader.jdbcUser)
      .option("password", ConfigLoader.jdbcPassword)
      .option("driver", ConfigLoader.jdbcDriver)
      .mode("append")
      .save()
  }
}

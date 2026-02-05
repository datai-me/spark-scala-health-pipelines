package persistence

import java.sql.DriverManager
import config.ConfigLoader

object JdbcUpsertWriter {

  def merge(): Unit = {

    val conn = DriverManager.getConnection(
      ConfigLoader.jdbcUrl,
      ConfigLoader.jdbcUser,
      ConfigLoader.jdbcPassword
    )

    conn.setAutoCommit(false)

    val stmt = conn.createStatement()

    val sql =
      if (ConfigLoader.jdbcDriver.contains("postgresql"))
        PostgresSql.mergeSql
      else
        MySql.mergeSql

    stmt.execute(sql)
    conn.commit()

    stmt.close()
    conn.close()
  }
}

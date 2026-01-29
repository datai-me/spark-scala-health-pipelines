// -------------------------------------------------------------
// FICHIER : pipeline/EpidemicPipelineApp.scala
// RÔLE    : Point d'entrée du pipeline
// -------------------------------------------------------------

package pipeline

import loader.EpidemicSparkLoader
import sql.EpidemicSqlRunner

/**
 * Orchestrateur principal du pipeline
 */
object EpidemicPipelineApp {

  def run(): Unit = {

    // 1. Chargement des données depuis l’API publique
    val dataset = EpidemicSparkLoader.load()

    // 2. Exécution des requêtes Spark SQL + affichage console
    EpidemicSqlRunner.run(dataset)
  }
}

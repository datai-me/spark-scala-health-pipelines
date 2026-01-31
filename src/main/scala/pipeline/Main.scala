// =============================================================
// PROJET : EPIDEMIC BIG DATA PIPELINE
// STACK  : Scala 2.13 | Spark 4.x | Delta Lake
// CAS    : Surveillance épidémiologique à partir d'une API publique
// =============================================================

import utils.VersionsInfo

package pipeline

/**
 * Point d'entrée JVM du projet.
 * C'est CE fichier qui est exécuté par sbt / spark-submit.
 */
object Main {

  def main(args: Array[String]): Unit = {
   
    println("===================================")
    println(" Epidemic Health Pipeline START ")
    println("===================================")
	
	 // 🔹 Affichage des versions AU DÉMARRAGE
    VersionsInfo.printVersions(spark)
	
    // 🔹 Lancement du pipeline principal
    EpidemicPipelineApp.run(spark)

    println("===================================")
    println(" Epidemic Big Health Pipeline END ")
    println("===================================")
  }
}

// =============================================================
// FIN DU PROJET – PIPELINE ÉPIDÉMIOLOGIQUE COMPLET
// =============================================================
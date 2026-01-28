name := "spark-scala-health-pipelines"

version := "1.0.0"

// Scala 3 (choisir une vraie version existante)
scalaVersion := "2.13.12"

// SET JDK to 17
javacOptions ++= Seq("-source", "17", "-target", "17")

// IMPORTANT : compatibilité avec libs Scala 2.13
scalacOptions ++= Seq(
  "-language:implicitConversions"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "io.delta" %% "delta-spark" % "3.1.0",
  "org.apache.httpcomponents.client5" % "httpclient5" % "5.3.1"
)

// Force la compatibilité Scala 2.13
scalaBinaryVersion := "2.13"
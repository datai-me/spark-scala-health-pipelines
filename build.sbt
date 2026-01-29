name := "spark-scala-health-pipelines"

version := "1.0.0"

// Scala 3 (choisir une vraie version existante)
scalaVersion := "2.13.18"

fork := true

// Aligner version JDK sur 17
javacOptions ++= Seq(
  "-source", "17", "-target", "17",  
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)

// IMPORTANT : compatibilité avec libs Scala 2.13
scalacOptions ++= Seq(
  "-language:implicitConversions"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.1",
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "com.lihaoyi" %% "requests" % "0.8.0",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.3",
  "io.delta" %% "delta-spark" % "3.1.0",
  "org.apache.httpcomponents.client5" % "httpclient5" % "5.3.1"
)

// Force la compatibilité Scala 2.13
scalaBinaryVersion := "2.13"
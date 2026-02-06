name := "spark-scala-health-pipelines"

version := "1.0.0"

// Scala 3 (choisir une vraie version existante)
scalaVersion := "2.13.18"

fork := true

// IMPORTANT : compatibilité avec libs Scala 2.13
scalacOptions ++= Seq(
  "-language:implicitConversions"
)

libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "42.7.3",
  "com.mysql" % "mysql-connector-j" % "9.6.0", // Use the latest version
  "com.typesafe" % "config" % "1.4.3",
  "org.apache.logging.log4j" % "log4j-api" % "2.23.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.23.1",
  "org.apache.spark" %% "spark-sql" % "3.5.8",
  "org.apache.spark" %% "spark-core" % "3.5.8",
  "org.apache.spark" %% "spark-mllib" % "3.5.8",
  "com.lihaoyi" %% "requests" % "0.8.0",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.3",
  "io.delta" %% "delta-spark" % "3.1.0",
  "org.apache.httpcomponents.client5" % "httpclient5" % "5.3.1",
  "com.typesafe.akka" %% "akka-actor-typed" % "2.8.5",
  "com.typesafe.akka" %% "akka-stream"      % "2.8.5",
  "com.typesafe.akka" %% "akka-http"        % "10.5.3",
  "org.scalatest" %% "scalatest" % "3.2.18" % Test
)

// Force la compatibilité Scala 2.13
scalaBinaryVersion := "2.13"
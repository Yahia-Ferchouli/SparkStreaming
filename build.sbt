ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "SparkStreaming",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.3.2",
      "org.apache.spark" %% "spark-streaming" % "3.3.2",
      "org.apache.spark" %% "spark-sql" % "3.3.2", // Ajout de la dépendance Spark SQL
      "mysql" % "mysql-connector-java" % "8.0.28"
    )
  )

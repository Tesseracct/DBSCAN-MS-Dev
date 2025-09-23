ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.16"

lazy val root = (project in file("."))
  .settings(
    name := "DBSCAN-MS"
  )

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "4.0.1" % "provided",
                            "org.apache.spark" %% "spark-sql" % "4.0.1" % "provided",
                            "org.scalanlp" %% "breeze" % "2.1.0",
                            "org.scalanlp" %% "breeze-viz" % "2.1.0",
                            "org.scalatest" %% "scalatest" % "3.2.19" % Test,
                            "org.jgrapht" % "jgrapht-core" % "1.5.2")


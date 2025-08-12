ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.16"

lazy val root = (project in file("."))
  .settings(
    name := "DBSCAN-MS"
  )

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "3.5.6",
                            "org.apache.spark" %% "spark-sql"  % "3.5.6",
                            "org.scalanlp" %% "breeze" % "2.1.0",
                            "org.scalanlp" %% "breeze-viz" % "2.1.0",
                            "org.apache.spark" %% "spark-core" % "3.5.1" % "provided",
                            "org.scalatest" %% "scalatest" % "3.2.19" % Test)


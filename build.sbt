ThisBuild / name := "DBSCAN-MS"

ThisBuild / version := "0.1"

ThisBuild / scalaVersion := "2.13.16"

lazy val root = (project in file("."))
  .settings(
    name := "DBSCAN-MS"
  )

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "4.0.1" % "provided",
                            "org.apache.spark" %% "spark-sql" % "4.0.1" % "provided",
                            "org.scalatest" %% "scalatest" % "3.2.19" % Test,
                            "org.jgrapht" % "jgrapht-core" % "1.5.2")

// Assembly settings
assembly / mainClass := Some("app.Main")
assembly / assemblyJarName := "dbscan_ms.jar"
assembly / test := {}
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
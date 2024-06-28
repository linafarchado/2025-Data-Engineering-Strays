import sbtassembly.AssemblyPlugin.autoImport._

ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "part4",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.2.0" % "provided",
      "org.apache.spark" %% "spark-core" % "3.2.0" % "provided",
      "org.apache.hadoop" % "hadoop-client" % "3.2.0"
    ),
    assembly / mainClass := Some("DroneDataAnalyzer"),
    assembly / assemblyJarName := "drone-data-analyzer.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "application.conf"            => MergeStrategy.concat
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )

Compile / run / mainClass := Some("DroneDataAnalyzer")
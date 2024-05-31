ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.3.3"

lazy val root = (project in file("."))
  .settings(
    name := "part1",
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % "3.2.0",
      "io.spray" %% "spray-json" % "1.3.6",
      "ch.qos.logback" % "logback-classic" % "1.4.6"
    )
  )
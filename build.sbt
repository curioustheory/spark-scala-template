import Dependencies._

lazy val commonSettings = Seq(
  organization := "com.curioustheory",
  version := "0.0.1",
  scalaVersion := "2.11.8"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "spark-scala-template",
    libraryDependencies ++= backendDeps
  )
  
import sbt._

object Dependencies {
  // Versions
  lazy val sparkVersion = "2.0.1"
  lazy val jodaTimeVersion = "2.9.7"

  // Libraries
  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
  val jodaTime = "joda-time" % "joda-time" % jodaTimeVersion

  // Projects
  val backendDeps = Seq(
    sparkCore,
    sparkSql,
    jodaTime)
}
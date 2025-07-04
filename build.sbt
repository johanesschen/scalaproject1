ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "scalaproject001"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql"  % "3.5.1",
  "log4j" % "log4j" % "1.2.17",
  "com.typesafe" % "config" % "1.4.3",
  "org.scalatest" %% "scalatest" % "3.2.18" % Test
)

Test / fork := true
Test / javaOptions += "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
Test / javaOptions += "--add-exports=java.base/sun.security.action=ALL-UNNAMED"


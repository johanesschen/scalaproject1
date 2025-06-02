ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "scalaproject001",
    idePackagePrefix := Some("org.scalaproject001.application")
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql"  % "3.5.1",
  "com.typesafe" % "config" % "1.4.3",
  "log4j" % "log4j" % "1.2.17",
  "org.scalatest" %% "scalatest" % "3.2.18" % Test
)

Test / fork := true

Test / javaOptions += "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"

Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat

Test / javaOptions += "--add-exports=java.base/sun.security.action=ALL-UNNAMED"

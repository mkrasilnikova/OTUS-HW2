import Dependencies._

ThisBuild / scalaVersion     := "2.13.4"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "homework2",
    libraryDependencies ++= Seq(scalaTest % Test),
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % "3.2.1"
    )


  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.

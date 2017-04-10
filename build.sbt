import sbt.Keys._

val projectScalaVersion = "2.11.8"

val spannerClientLibraries = Seq(
  "com.google.cloud" % "google-cloud-spanner" % "0.12.0-beta",
  "com.google.auth" % "google-auth-library-oauth2-http" % "0.6.0",
  "com.google.guava" % "guava" % "21.0"
) 

val loggingLibraries = Seq(
  "ch.qos.logback" % "logback-classic" % "1.1.7"
)

val testLibraries = Seq(
  "org.specs2" %% "specs2-core" % "3.6.2" % "test",
  "org.specs2" %% "specs2-mock" % "3.6.2" % "test",
  "com.typesafe" % "config" % "1.3.1" % "test"
)

val commonLibraries = Seq(
  "joda-time" % "joda-time" % "2.9.6",
  "org.joda" % "joda-convert" % "1.8"
)

parallelExecution in ThisBuild := false

val projectVersion = "0.3"

lazy val root = (project in file("."))
  .settings(antlr4Settings : _*)
  .settings(
    scalaVersion := projectScalaVersion,
    name := "bolt",
    organization := "com.sopranoworks",
    version := projectVersion,
    publishTo := Some(Resolver.file("codebook",file("../RustyRaven.github.io"))(Patterns(true, Resolver.mavenStyleBasePattern))),
    antlr4PackageName in Antlr4 := Some("com.sopranoworks.bolt"),
    libraryDependencies ++=
      spannerClientLibraries ++
      loggingLibraries ++
      testLibraries ++
      commonLibraries
  )
  .enablePlugins(JavaAppPackaging)

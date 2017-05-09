import sbt.Keys._

val projectScalaVersion = "2.11.8"

resolvers in Global += "RustyRaven" at "http://rustyraven.github.io"

resolvers in Global += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"

val codebookLibrary = Seq("com.rustyraven" %% "codebook-runtime" % "1.2-SNAPSHOT")

val spannerClientLibraries = Seq(
  "com.google.cloud" % "google-cloud-spanner" % "0.17.2-beta",
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

val projectVersion = "0.4-SNAPSHOT"

lazy val root = (project in file("."))
  .settings(antlr4Settings : _*)
  .settings(
    scalaVersion := projectScalaVersion,
    name := "bolt",
    organization := "com.sopranoworks",
    version := projectVersion,
    publishTo := Some(Resolver.file("bolt",file("../RustyRaven.github.io"))(Patterns(true, Resolver.mavenStyleBasePattern))),
    antlr4PackageName in Antlr4 := Some("com.sopranoworks.bolt"),
    libraryDependencies ++=
      spannerClientLibraries ++
      codebookLibrary ++
      loggingLibraries ++
      testLibraries ++
      commonLibraries,
    dependencyOverrides += "io.netty" % "netty-tcnative-boringssl-static" % "1.1.33.Fork22"
  )
  .enablePlugins(JavaAppPackaging)

import sbt.Keys._

val projectScalaVersion = "2.11.8"

val scalaVersions = Seq("2.11.8", "2.12.2")

resolvers in Global += "RustyRaven" at "http://rustyraven.github.io"

resolvers in Global += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"

val codebookLibrary = Seq("com.rustyraven" %% "codebook-runtime" % "1.3-SNAPSHOT")

val spannerClientLibraries = Seq(
  "com.google.cloud" % "google-cloud-spanner" % "0.19.0-beta",
  "com.google.auth" % "google-auth-library-oauth2-http" % "0.6.0",
  "com.google.guava" % "guava" % "21.0"
) 

val shapelessLibrary = Seq(
  "com.chuusai" %% "shapeless" % "2.3.2"
)

val loggingLibraries = Seq(
  "ch.qos.logback" % "logback-classic" % "1.1.7"
)

val scoptLibrary = Seq("com.github.scopt" %% "scopt" % "3.5.0")

val testLibraries = Seq(
  "org.specs2" %% "specs2-core" % "3.8.9" % "test",
  "org.specs2" %% "specs2-mock" % "3.8.9" % "test",
  "com.typesafe" % "config" % "1.3.1" % "test"
)

val commonLibraries = Seq(
  "joda-time" % "joda-time" % "2.9.6",
  "org.joda" % "joda-convert" % "1.8"
)

val jlineLibrary = Seq("jline" % "jline" % "2.14.3")

parallelExecution in ThisBuild := false

fork in run := true

val projectVersion = "0.8.2b-SNAPSHOT"

val noJavaDoc = Seq(
  publishArtifact in (Compile, packageDoc) := false,
  publishArtifact in packageDoc := false,
  publishArtifact in packageSrc := false,
  sources in (Compile,doc) := Seq.empty
)

lazy val core = (project in file("."))
  .settings(antlr4Settings : _*)
  .settings(
    scalaVersion := projectScalaVersion,
    crossScalaVersions := scalaVersions,
    name := "bolt",
    organization := "com.sopranoworks",
    version := projectVersion,
    publishTo := Some(Resolver.file("bolt",file("../RustyRaven.github.io"))(Patterns(true, Resolver.mavenStyleBasePattern))),
    antlr4PackageName in Antlr4 := Some("com.sopranoworks.bolt"),
    libraryDependencies ++=
      spannerClientLibraries ++
      codebookLibrary ++
      shapelessLibrary ++
      loggingLibraries ++
      testLibraries ++
      commonLibraries,
    dependencyOverrides += "io.netty" % "netty-tcnative-boringssl-static" % "1.1.33.Fork22"
  )
  .settings(noJavaDoc: _*)

lazy val client = (project in file("client"))
  .enablePlugins(JavaAppPackaging,UniversalPlugin)
  .settings(
    scalaVersion := projectScalaVersion,
    name := "spanner-cli",
    version := projectVersion,
    assemblyJarName := "spanner-cli.jar",
    scriptClasspath := Seq( assemblyJarName.value ),
    mappings in Universal := {
        val universalMappings = (mappings in Universal).value
        val fatJar = (assembly in Compile).value
        val filtered = universalMappings filter {
            case (file, name) =>  ! name.endsWith(".jar")
        }
        filtered :+ (fatJar -> ("lib/" + fatJar.getName))
    },
    assemblyMergeStrategy in assembly := {
      case "META-INF/io.netty.versions.properties" => MergeStrategy.concat
      case "project.properties" => MergeStrategy.first
      case "META-INF/native/linux32/libjansi.so" => MergeStrategy.last
      case "META-INF/native/linux64/libjansi.so" => MergeStrategy.last
      case "META-INF/native/osx/libjansi.jnilib" => MergeStrategy.last
      case "META-INF/native/windows32/jansi.dll" => MergeStrategy.last
      case "META-INF/native/windows64/jansi.dll" => MergeStrategy.last
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
    libraryDependencies ++= scoptLibrary ++ jlineLibrary
  ).dependsOn(core)
  .settings(noJavaDoc: _*)

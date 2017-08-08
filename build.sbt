import sbt.Keys._

val projectScalaVersion = "2.11.8"

val scalaVersions = Seq("2.11.8", "2.12.2")

resolvers in Global += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"

val codebookLibrary = Seq("com.rusty-raven" %% "codebook-runtime" % "1.3.3" excludeAll(
  ExclusionRule(organization = "com.typesafe.akka"),
  ExclusionRule(organization = "net.liftweb")
))

val spannerClientLibraries = Seq(
//  "com.google.cloud" % "google-cloud-spanner" % "0.20.3-beta",  // not work with SIGILL hack
  "com.google.cloud" % "google-cloud-spanner" % "0.19.0-beta",
  "com.google.auth" % "google-auth-library-oauth2-http" % "0.6.0",
  "com.google.guava" % "guava" % "21.0"
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

val projectVersion = "0.15-SNAPSHOT"

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
    antlr4PackageName in Antlr4 := Some("com.sopranoworks.bolt"),
    libraryDependencies ++=
      spannerClientLibraries ++
      codebookLibrary ++
      loggingLibraries ++
      testLibraries ++
      commonLibraries,
    dependencyOverrides += "io.netty" % "netty-tcnative-boringssl-static" % "1.1.33.Fork22",  // for SIGILL hack on old intel CPUs
    pomExtra :=
      <url>https://github.com/OsamuTakahashi/bolt</url>
        <licenses>
          <license>
            <name>MIT</name>
            <url>https://opensource.org/licenses/MIT</url>
          </license>
        </licenses>
        <scm>
          <url>https://github.com/OsamuTakahashi/bolt</url>
          <connection>https://github.com/OsamuTakahashi/bolt.git</connection>
        </scm>
        <developers>
          <developer>
            <id>OsamuTakahashi</id>
            <name>Osamu Takahashi</name>
            <url>https://github.com/OsamuTakahashi/</url>
          </developer>
        </developers>
  )

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
    libraryDependencies ++= scoptLibrary ++ jlineLibrary,
    dependencyOverrides += "io.netty" % "netty-tcnative-boringssl-static" % "1.1.33.Fork22"  // for SIGILL hack on old intel CPUs
  ).dependsOn(core)
  .settings(noJavaDoc: _*)

lazy val dump = (project in file("dump"))
  .enablePlugins(JavaAppPackaging,UniversalPlugin)
  .settings(
    scalaVersion := projectScalaVersion,
    name := "spanner-dump",
    version := projectVersion,
    assemblyJarName := "spanner-dump.jar",
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
    libraryDependencies ++= scoptLibrary ++ jlineLibrary,
    dependencyOverrides += "io.netty" % "netty-tcnative-boringssl-static" % "1.1.33.Fork22"  // for SIGILL hack on old intel CPUs
  ).dependsOn(core)
  .settings(noJavaDoc: _*)

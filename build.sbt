import com.simplytyped.Antlr4Plugin
import sbt.Keys._

val projectScalaVersion = "2.12.9"

val scalaVersions = Seq("2.12.9")

resolvers in Global += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

//resolvers in Global += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"

resolvers += "confluent" at "https://packages.confluent.io/maven/"

val scalaLibrary = Seq("org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2")

val spannerClientLibraries = Seq(
//  "com.google.cloud" % "google-cloud-spanner" % "2.0.2"
  "com.google.cloud" % "google-cloud-spanner" % "6.63.0"
)

val scioVersion = "0.11.1"
val beamVersion = "2.54.0"

val scioLibraries = Seq(
  "com.spotify" %% "scio-core" % scioVersion,
  "com.spotify" %% "scio-test" % scioVersion % "test",
  "org.apache.beam" % "beam-runners-direct-java" % beamVersion % Test,
  "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
  "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion
)

val loggingLibraries = Seq(
  "ch.qos.logback" % "logback-classic" % "1.1.7"
)

val scoptLibrary = Seq("com.github.scopt" %% "scopt" % "4.1.0")

val testLibraries = Seq(
  "org.specs2" %% "specs2-core" % "4.6.0" % "test",
  "org.specs2" %% "specs2-mock" % "4.6.0" % "test",
  "com.typesafe" % "config" % "1.3.1" % "test"
)

val commonLibraries = Seq(
  "joda-time" % "joda-time" % "2.12.7",
  "org.joda" % "joda-convert" % "2.2.3",
  "org.apache.commons" % "commons-text" % "1.11.0"
)

val jlineLibrary = Seq("jline" % "jline" % "2.14.6")

ThisBuild / parallelExecution := false

fork in run := true

//val projectVersion = "0.24.0-SNAPSHOT"
val projectVersion = "0.24.0"

val noJavaDoc = Seq(
  publishArtifact in (Compile, packageDoc) := false,
  publishArtifact in packageDoc := false,
  publishArtifact in packageSrc := false,
  sources in (Compile,doc) := Seq.empty
)

lazy val core = (project in file("."))
//  .settings(antlr4Settings : _*)
  .settings(
    scalaVersion := projectScalaVersion,
    crossScalaVersions := scalaVersions,
    name := "bolt",
    organization := "com.sopranoworks",
    version := projectVersion,
    antlr4PackageName in Antlr4 := Some("com.sopranoworks.bolt"),
    libraryDependencies ++=
      scalaLibrary ++
      spannerClientLibraries ++
      loggingLibraries ++
      testLibraries ++
      commonLibraries,
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
  ).enablePlugins(Antlr4Plugin)

lazy val client = (project in file("client"))
  .enablePlugins(JavaAppPackaging,UniversalPlugin)
  .enablePlugins(NativeImagePlugin)
  .settings(
    scalaVersion := projectScalaVersion,
    name := "spanner-cli",
    version := projectVersion,
    Compile / mainClass := Some("com.sopranoworks.bolt.Main"),
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
      case "module-info.class" => MergeStrategy.last
      case "META-INF/native-image/native-image.properties" => MergeStrategy.last
      case "META-INF/versions/9/module-info.class" => MergeStrategy.last
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
    nativeImageOptions ++= List("-H:+AllowIncompleteClasspath"),
    libraryDependencies ++= scoptLibrary ++ jlineLibrary
  ).dependsOn(core)
  .settings(noJavaDoc: _*)

lazy val dump = (project in file("dump"))
  .enablePlugins(JavaAppPackaging,UniversalPlugin)
  .enablePlugins(NativeImagePlugin)
  .settings(
    scalaVersion := projectScalaVersion,
    name := "spanner-dump",
    version := projectVersion,
    Compile / mainClass := Some("com.sopranoworks.bolt.Main"),
    assemblyJarName := "spanner-dump.jar",
    scriptClasspath := Seq( assemblyJarName.value ),
    mappings in Universal := {
        val universalMappings = (mappings in Universal).value
        val fatJar = (assembly in Compile).value
        val filtered = universalMappings filter {
            case (file, name) =>  ! name.endsWith(".jar") && ! file.getPath.contains("kotlin")
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
      case "META-INF/native-image/io.netty/transport/reflection-config.json" => MergeStrategy.last
      case "google/protobuf/compiler/plugin.proto" => MergeStrategy.last
      case "google/protobuf/descriptor.proto" => MergeStrategy.last
      case "google/protobuf/duration.proto" => MergeStrategy.last
      case "google/protobuf/timestamp.proto" => MergeStrategy.last
      case "google/protobuf/any.proto" => MergeStrategy.first
      case "google/protobuf/field_mask.proto" => MergeStrategy.first
      case "google/protobuf/wrappers.proto" => MergeStrategy.first
      case "module-info.class" => MergeStrategy.first
      case "mozilla/public-suffix-list.txt" => MergeStrategy.first
      case "codegen/config.fmpp" => MergeStrategy.first
      case "git.properties" => MergeStrategy.first
      case PathList("google", "api", _ @ _ *) => MergeStrategy.last
      case PathList("google", "type", _ @ _ *) => MergeStrategy.last
      case PathList("google", "cloud", _ @ _ *) => MergeStrategy.last
      case PathList("com","google","bigtable", _ @ _*) => MergeStrategy.first
      case PathList("com","google","cloud","bigtable", _ @ _*) => MergeStrategy.first
      case PathList("com","google","protobuf", _ @ _ *) => MergeStrategy.first
      case PathList("com","google","errorprone", _ @ _ *) => MergeStrategy.first
      case PathList("com","twitter","algebird", _ @ _*) => MergeStrategy.last
      case PathList("org","apache","beam","sdk","coders", _ @ _*) => MergeStrategy.last
      case PathList("org","apache","commons", _ @ _*) => MergeStrategy.last
      case PathList("com","squareup","kotlinpoet", _ @ _*) => MergeStrategy.first
      case PathList("com","squareup","wire", _ @ _*) => MergeStrategy.last
      case PathList("commonMain","default", _ @ _*) => MergeStrategy.last
      case PathList("org","slf4j", _ @ _*) => MergeStrategy.last
      case PathList("META-INF" , _ @ _*) => MergeStrategy.last
      case PathList(ps @ _ *) if ps.last.endsWith(".proto") => MergeStrategy.last
      case PathList(ps @ _ *) if ps.last == "native-image.properties" => MergeStrategy.last
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
    nativeImageOptions ++= List("-H:+AllowIncompleteClasspath",
      "-H:+TraceClassInitialization",
      "--initialize-at-build-time=org.apache.beam.vendor.guava.v26_0_jre.com.google.common.math.IntMath$1",
      "--initialize-at-build-time=ch.qos.logback.classic.Logger",
      "--initialize-at-build-time=ch.qos.logback.core.status.StatusBase",
      "--initialize-at-build-time=ch.qos.logback.core.status.InfoStatus",
      "--initialize-at-build-time=ch.qos.logback.core.spi.AppenderAttachableImpl",
      "--initialize-at-build-time=ch.qos.logback.classic.PatternLayout",
      "--initialize-at-build-time=ch.qos.logback.classic.Level",
      "--initialize-at-build-time=ch.qos.logback.core.CoreConstants",
      "--initialize-at-build-time=org.slf4j.impl.StaticLoggerBinder",
      "--initialize-at-build-time=javax.xml.parsers.FactoryFinder",
      "--initialize-at-build-time=org.slf4j.LoggerFactory",
      "--initialize-at-build-time=scala.Symbol$",
      "--initialize-at-build-time=jdk.xml.internal.SecuritySupport",
      "--initialize-at-run-time=io.netty.util.internal.logging.Log4JLogger",
      "--initialize-at-run-time=io.netty.handler.ssl.JdkNpnApplicationProtocolNegotiator",
      "--initialize-at-build-time=org.conscrypt.Conscrypt",
      "--initialize-at-build-time=org.conscrypt.NativeCrypto",
      "--initialize-at-build-time=org.conscrypt.HostProperties$Architecture",
      "--initialize-at-build-time=org.conscrypt.Platform",
      "--initialize-at-build-time=org.conscrypt.OpenSSLProvider",
      "--initialize-at-build-time=org.conscrypt.NativeLibraryLoader",
      "--initialize-at-build-time=org.conscrypt.HostProperties",
      "--initialize-at-run-time=io.netty.handler.ssl.PemPrivateKey"
    ),
    libraryDependencies ++= scoptLibrary ++ jlineLibrary ++ scioLibraries
  ).dependsOn(core)
  .settings(noJavaDoc: _*)

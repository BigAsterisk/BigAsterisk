// BigAsterisk — a unified debugging and testing platform for Apache Spark.
//
// Layout
//   modules/api      version-independent tool API + the SparkBinding SPI
//   modules/spark4   the Spark 4.x implementation of that SPI (lineage capture)
//   modules/bigsift  fault-inducing input isolation (provenance + delta debugging)
//   examples         runnable demos and benchmarks
//
// Every tool compiles against `api` only. Spark-version-specific code is confined to
// a binding module, resolved at runtime through java.util.ServiceLoader. Supporting a
// future Spark release means adding a `modules/sparkN` binding — no tool changes.

ThisBuild / organization := "edu.vt.bigasterisk"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.18"

val sparkVersion = "4.1.2"

// JDK 17+ module opens that Spark 4 requires at runtime.
val sparkJavaOptions = Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)

lazy val commonSettings = Seq(
  scalacOptions ++= Seq("-deprecation", "-unchecked"),
  javacOptions ++= Seq("--release", "17"),
  Test / fork := true,
  Test / javaOptions ++= sparkJavaOptions
)

// Spark as a compile-time-only dependency: BigAsterisk attaches to the user's Spark.
lazy val sparkProvided = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql"  % sparkVersion % Provided
)

lazy val sparkTest = Seq(
  "org.scalatest"    %% "scalatest"  % "3.2.19"     % Test,
  "org.apache.spark" %% "spark-core" % sparkVersion % Test,
  "org.apache.spark" %% "spark-sql"  % sparkVersion % Test
)

lazy val api = (project in file("modules/api"))
  .settings(
    name := "bigasterisk-api",
    commonSettings,
    libraryDependencies ++= sparkProvided ++ sparkTest
  )

lazy val spark4 = (project in file("modules/spark4"))
  .dependsOn(api)
  .settings(
    name := "bigasterisk-spark4",
    commonSettings,
    libraryDependencies ++= sparkProvided ++ sparkTest ++ Seq(
      // Shipped by Spark itself; compile against the same artifacts.
      "org.roaringbitmap" % "RoaringBitmap" % "1.2.1"     % Provided,
      "com.google.guava"  % "guava"         % "33.4.0-jre" % Provided,
      "it.unimi.dsi"      % "fastutil"      % "8.5.15"
    ),
    // local-cluster tests launch real executor JVMs via Spark's launcher, which needs
    // a Spark binary distribution at SPARK_HOME plus the Scala version that
    // bin/load-spark-env.sh would otherwise export.
    Test / envVars ++= Map(
      "SPARK_HOME" ->
        ((ThisBuild / baseDirectory).value / "tools" / "spark-4.1.2-bin-hadoop3").getAbsolutePath,
      "SPARK_SCALA_VERSION" -> "2.13"
    )
  )

lazy val bigsift = (project in file("modules/bigsift"))
  .dependsOn(api, spark4 % "compile->compile;test->test")
  .settings(
    name := "bigasterisk-bigsift",
    commonSettings,
    libraryDependencies ++= sparkProvided ++ sparkTest,
    Test / envVars ++= Map(
      "SPARK_HOME" ->
        ((ThisBuild / baseDirectory).value / "tools" / "spark-4.1.2-bin-hadoop3").getAbsolutePath,
      "SPARK_SCALA_VERSION" -> "2.13"
    )
  )

// OptDebug is arithmetic over provenance: it reaches Spark only through `api`, so it
// carries no dependency on a Spark version. `spark4` is a test-only dependency, there
// to supply a binding for the suite to run against.
lazy val optdebug = (project in file("modules/optdebug"))
  .dependsOn(api, spark4 % "test->compile;test->test")
  .settings(
    name := "bigasterisk-optdebug",
    commonSettings,
    libraryDependencies ++= sparkProvided ++ sparkTest,
    Test / envVars ++= Map(
      "SPARK_HOME" ->
        ((ThisBuild / baseDirectory).value / "tools" / "spark-4.1.2-bin-hadoop3").getAbsolutePath,
      "SPARK_SCALA_VERSION" -> "2.13"
    )
  )

lazy val examples = (project in file("examples"))
  .dependsOn(api, spark4, bigsift, optdebug)
  .settings(
    name := "bigasterisk-examples",
    commonSettings,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql"  % sparkVersion
    ),
    Compile / run / fork := true,
    Compile / run / javaOptions ++= sparkJavaOptions,
    // run example mains from the repo root so relative paths (tpcds/data, ...) resolve
    Compile / run / baseDirectory := (ThisBuild / baseDirectory).value,
    // forward stdin so interactive mains read input
    Compile / run / connectInput := true
  )

// One Scaladoc site across every module: `sbt unidoc` -> target/scala-2.13/unidoc.
// The manual (mkdocs) links to it as api/index.html; see docs/install.md.
lazy val root = (project in file("."))
  .enablePlugins(ScalaUnidocPlugin)
  .aggregate(api, spark4, bigsift, optdebug, examples)
  .settings(
    name := "bigasterisk",
    publish / skip := true,
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(api, spark4, bigsift, optdebug),
    ScalaUnidoc / unidoc / scalacOptions ++= Seq(
      "-doc-title", "BigAsterisk — debugging and testing for Apache Spark",
      "-doc-version", version.value,
      "-doc-root-content", ((ThisBuild / baseDirectory).value / "rootdoc.txt").getAbsolutePath,
      "-groups",
      // document the capture engine too: these docs are meant to explain the code,
      // not only its public surface
      "-private",
      "-no-link-warnings"
    )
  )

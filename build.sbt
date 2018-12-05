
name := """wndservice"""
organization := "Office Depot"

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.12"

libraryDependencies += guice

val jacksonVersion = "2.6.7"
val sparkVersion = "2.3.1"
//val bigDLVersion = "0.7.0"
val analyticsZooVersion = "0.3.0"
//val bigDLVersion = "0.7.1"
//libraryDependencies += "com.intel.analytics.bigdl" % "bigdl-SPARK_2.3" % "0.7.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

//libraryDependencies += "com.intel.analytics.bigdl" %% "bigdl-spark" % "1.0.0" from "file:///Users/luyangwang/.m2/repository/com/intel/analytics/bigdl/bigdl-spark/1.0.0/bigdl-spark-1.0.0.jar"

libraryDependencies += "com.intel.analytics.zoo" % "analytics-zoo-bigdl_0.7.1-spark_2.3.1" % analyticsZooVersion

libraryDependencies ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion
)

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % jacksonVersion
dependencyOverrides += "com.google.guava" % "guava" % "17.0"

libraryDependencies += "com.google.guava" % "guava" % "12.0"
libraryDependencies += "com.intel.analytics.zoo" % "analytics-zoo-bigdl_0.7.1-spark_2.3.1" % analyticsZooVersion
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.5"
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.354"
//libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-s3" % "0.9"
//libraryDependencies ++= Seq(
//  "com.amazonaws" % "aws-java-sdk" % "1.11.354"
//)

//libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.6"


mainClass in assembly := Some("play.core.server.ProdServerStart")
fullClasspath in assembly += Attributed.blank(PlayKeys.playPackageAssets.value)

import com.typesafe.sbt.packager.MappingsHelper._
mappings in Universal ++= directory(baseDirectory.value / "modelFiles")

assemblyMergeStrategy in assembly := {
  case manifest if manifest.contains("MANIFEST.MF") =>
    // We don't need manifest files since sbt-assembly will create
    // one with the given settings
    MergeStrategy.discard
  case referenceOverrides if referenceOverrides.contains("reference-overrides.conf") =>
    // Keep the content for all reference-overrides.conf files
    MergeStrategy.concat
  case x =>
    // For all the other files, use the default sbt-assembly merge strategy
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}


// Adds additional packages into Twirl
//TwirlKeys.templateImports += "Office Depot.controllers._"

// Adds additional packages into conf/routes
// play.sbt.routes.RoutesKeys.routesImport += "Office Depot.binders._"

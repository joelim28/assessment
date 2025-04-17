scalaVersion := "2.12.12"

name := "Assessment"
organization := "ch.epfl.scala"
version := "1.0"

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-parser-combinators" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "3.1.2" exclude("org.scala-lang.modules", "scala-parser-combinators"),
  "org.apache.spark" %% "spark-core" % "3.1.2",
  "com.github.mjakubowski84" %% "parquet4s-core" % "1.6.0",
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "org.scala-lang" % "scala-compiler" % scalaVersion.value
)

libraryDependencySchemes += "org.scala-lang.modules" %% "scala-parser-combinators" % VersionScheme.EarlySemVer

evictionErrorLevel := Level.Warn

fork in run := true
javaOptions in run ++= Seq(
  "-Dspark.master=local[*]",
  "-Dspark.app.name=AssessmentApp",
  "-Dspark.executor.memory=2g",
  "-Dspark.driver.memory=1g",
  "-Dspark.executor.cores=4",
  "-Dspark.dynamicAllocation.enabled=true",
  "-Dspark.dynamicAllocation.minExecutors=1",
  "-Dspark.dynamicAllocation.maxExecutors=10",
  "-Dspark.sql.autoBroadcastJoinThreshold=10MB",
  "-Dspark.serializer=org.apache.spark.serializer.KryoSerializer"
)

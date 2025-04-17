error id: `<error>`#`<error>`.
file:///C:/Users/josephlim/Projects/data-transformation/build.sbt
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -scalastyleConfig.
	 -scalastyleConfig#
	 -scalastyleConfig().
	 -scala/Predef.scalastyleConfig.
	 -scala/Predef.scalastyleConfig#
	 -scala/Predef.scalastyleConfig().
offset: 707
uri: file:///C:/Users/josephlim/Projects/data-transformation/build.sbt
text:
```scala
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

scalastyl@@eConfig := file("scalastyle-config.xml")
scalastyleFailOnError := true

```


#### Short summary: 

empty definition using pc, found symbol in pc: 
name := "spark-mllib"

version := "1.0"

scalaVersion := "2.11.8-local"

val scalaV = "2.11.8"

libraryDependencies ++= Seq(
  "org.scala-lang"    %   "scala-compiler"      % scalaV,
  "org.scala-lang"    %   "scala-library"       % scalaV,
  "org.scala-lang"    %   "scala-reflect"       % scalaV,
  "org.apache.spark" %% "spark-core" % "1.5.2" % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.5.2",
  "org.apache.commons" % "commons-lang3" % "3.0"
)

autoScalaLibrary := false

scalaHome := Some(file("/home/eduardo/programs/scala-2.11.8"))

mainClass := Some("main.scala.master.spark.MainSpark")

//libraryDependencies ++= Seq(
//    "org.scalatest" % "scalatest_2.10" % "2.0.M5b" % "test"
//)

EclipseKeys.withSource := true

//mainClass in (Compile, run) := Some("com.master.spark.MainSpark")

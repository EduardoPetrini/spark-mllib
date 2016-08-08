name := "spark-mllib_nb-cv"

version := "1.0"

scalaVersion := "2.10.5"

val scalaV = "2.10.5"

libraryDependencies ++= Seq(
  "org.scala-lang"    %   "scala-compiler"      % scalaV,
  "org.scala-lang"    %   "scala-library"       % scalaV,
  "org.scala-lang"    %   "scala-reflect"       % scalaV,
  "org.apache.spark" %% "spark-core" % "1.6.1" % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.6.1",
  "org.apache.commons" % "commons-lang3" % "3.0"
)

autoScalaLibrary := false

scalaHome := Some(file("/home/eduardo/programs/scala-2.10.5"))

mainClass := Some("main.scala.master.spark.main.MainSpark")

//libraryDependencies ++= Seq(
//    "org.scalatest" % "scalatest_2.10" % "2.0.M5b" % "test"
//)

EclipseKeys.withSource := true

//mainClass in (Compile, run) := Some("main.scala.master.spark.MainSpark")

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

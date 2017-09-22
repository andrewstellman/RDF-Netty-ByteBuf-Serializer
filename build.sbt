lazy val commonSettings = Seq(
  name := "nettybufrdf",
  organization := "com.stellmangreene",
  version := "1.0",
  scalaVersion := "2.12.2"
)

scalaVersion := "2.12.2"
scalacOptions ++= Seq("-feature")

libraryDependencies ++= Seq(
   "io.netty" % "netty-all" % "4.1.15.Final",
   "org.eclipse.rdf4j" % "rdf4j-runtime" % "2.2.2",

   // Log dependencies
   "org.slf4j" % "slf4j-api" % "1.7.25",
   "org.slf4j" % "slf4j-simple" % "1.7.25",
   "org.log4s" %% "log4s" % "1.3.5",

   // Test dependencies
   "org.scalatest" %% "scalatest" % "3.0.1" % "test",
   "com.github.pathikrit" %% "better-files" % "3.1.0" % "test"
)

// sbt-eclipse settings
EclipseKeys.withSource := true


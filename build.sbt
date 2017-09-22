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

   // Test dependencies
   "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

// sbt-eclipse settings
EclipseKeys.withSource := true


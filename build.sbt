import sbt.Keys.libraryDependencies

enablePlugins(JavaAppPackaging)
maintainer := "Mingfei Shao"
packageSummary := s"Akka ${version.value} Server"

lazy val root = (project in file(".")).
  settings(
    name := "CSC536_FinalProject",
    version := "1.0",
    scalaVersion := "2.12.4",
    resolvers += Resolver.bintrayRepo("hseeberger", "maven"),
    libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.12",
    libraryDependencies += "com.typesafe.akka" %% "akka-remote" % "2.5.12",
    libraryDependencies += "com.typesafe.akka" %% "akka-cluster" % "2.5.12",
    libraryDependencies ++= Vector(
      "de.heikoseeberger"         %% "constructr"                         % "0.19.0",
      "com.lightbend.constructr"  %% "constructr-coordination-zookeeper"  % "0.4.0",
    )
  )
name := "LoadCoordinates"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.0.0"
libraryDependencies += "com.typesafe" % "config" % "1.2.1"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"

name := "spark-detect-bot-project"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.2" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.2" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.3.2" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.3.2"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.2"
libraryDependencies += "com.google.code.gson" % "gson" % "2.6.2"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}





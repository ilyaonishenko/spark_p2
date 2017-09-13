
name := "spark-p2"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += "Clojars" at "http://clojars.org/repo"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "2.7.3" % "provided",
  "org.apache.spark" % "spark-core_2.11" % "2.2.0" % "provided",
  "org.apache.spark" % "spark-streaming_2.11" % "2.2.0" % "provided",
  "org.apache.spark" % "spark-sql_2.11" % "2.2.0" % "provided",
  "org.apache.spark" % "spark-hive_2.11" % "2.2.0" %"provided",
  "org.apache.kafka" % "kafka_2.11" % "0.11.0.0",
  "com.github.nscala-time" %% "nscala-time" % "2.16.0",
  "jpcap" % "jpcap" % "0.1.18-002"
  //  "org.scalatest" %% "scalatest_2.11" % "3.0.3" % "test"
)
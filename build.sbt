name := "sparkStringing2Hbase"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.0.0"

libraryDependencies += "org.apache.spark" % "spark-streaming-flume_2.11" % "2.0.0"

libraryDependencies ++= Seq(
  "org.apache.hbase" % "hbase" % "0.98.18-hadoop2",
  "org.apache.hbase" % "hbase-client" % "0.98.18-hadoop2",
  "org.apache.hbase" % "hbase-common" % "0.98.18-hadoop2",
  "org.apache.hbase" % "hbase-server" % "0.98.18-hadoop2"
)
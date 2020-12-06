name := "SparkWork"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.2.1"
val hbaseVersion = "1.2.12"
val hadoopVersion = "2.6.0"

libraryDependencies ++= Seq("org.apache.hbase" % "hbase-server",
  "org.apache.hbase" % "hbase-client",
  "org.apache.hbase" % "hbase-common",
  "org.apache.hbase" % "hbase-hadoop-compat", // for generating Hfiles
   "org.apache.hbase" %"hbase-protocol"
).map(_ % hbaseVersion)

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion
libraryDependencies += "com.typesafe" % "config" % "1.3.3"

libraryDependencies ++= Seq("org.apache.spark" % "spark-streaming_2.11",
  "org.apache.spark" % "spark-sql_2.11",
  "org.apache.spark" %% "spark-core").map(_ % sparkVersion)
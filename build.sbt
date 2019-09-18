name := "SqlServerStreamingSource"

version := "0.1"

scalaVersion := "2.11.8"

lazy val excludeJpountz = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")
libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "3.2.0",
  "org.apache.hbase" % "hbase-common" % "2.1.5",
  "org.apache.hbase" % "hbase-client" % "2.1.5" ,
  "net.sourceforge.jtds" % "jtds" % "1.3.1",
  "com.microsoft.sqlserver" % "mssql-jdbc" % "7.2.1.jre8",
  "com.typesafe" % "config" % "1.3.4",
  "org.json" % "json" % "20180813",
  "org.json4s" %% "json4s-native" % "3.6.5",
  "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0" % "provided" excludeAll(excludeJpountz),
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.scalaj" %% "scalaj-http" % "2.4.2"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
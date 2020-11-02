name := "Scala_FinalProject_Bixi_Sprint3"

version := "0.3"

scalaVersion := "2.12.0"

val hadoopVersion = "2.7.3"

val btversion = "3.9.0"

val sparkVersion = "2.4.5"
/*
"organization" % "artifact" % "version"
 */

libraryDependencies += "au.com.bytecode" % "opencsv" % "2.4"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion
libraryDependencies += "org.apache.hive" % "hive-jdbc" % "1.1.0-cdh5.16.2"
libraryDependencies += "org.apache.spark" %% "spark-core"                 % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql"                  % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-streaming"            % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-mllib"                % sparkVersion
libraryDependencies += "com.github.agourlay" %% "json-2-csv" % "0.4.0"
libraryDependencies += "com.github.pathikrit" %% "better-files" % btversion
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.3.1"
libraryDependencies += "org.apache.kafka" % "kafka_2.12" % "2.5.1"

resolvers += "Cloudera" at "http://repository.cloudera.com/artifactory/cloudera-repos/"

/*libraryDependencies ++= Seq (
  "org.apache.hadoop" % "hadoop-common",
  "org.apache.hadoop" % "hadoop-hdfs",
).map( _ % hadoopVersion)*/
name := "project"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.2.1",
    "org.apache.spark" %% "spark-sql" % "2.2.1",
    "org.apache.spark" % "spark-streaming_2.11" % "2.2.1",
    "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.2.1",
    ("com.datastax.spark" %% "spark-cassandra-connector" % "2.0.2").exclude("io.netty", "netty-handler"),
    ("com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0").exclude("io.netty", "netty-handler"),
    "com.google.code.gson" % "gson" % "2.8.6",
    "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.2.1"
)
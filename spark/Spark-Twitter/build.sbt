name := "spark-twitter"

version := "1.0"

scalaVersion := "2.12.10"

resolvers += "spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
val SPARK_VERSION = "3.0.1"


libraryDependencies += "org.apache.spark" %% "spark-streaming" % SPARK_VERSION % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % SPARK_VERSION

libraryDependencies += "org.apache.spark" %% "spark-sql" % SPARK_VERSION

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % SPARK_VERSION

val CIRCE_VERSION = "0.7.0"

libraryDependencies ++= Seq(
  "io.circe"  %% "circe-core"     % CIRCE_VERSION,
  "io.circe"  %% "circe-generic"  % CIRCE_VERSION,
  "io.circe"  %% "circe-parser"   % CIRCE_VERSION
)

name := "SparkStreaming"

version := "0.1"

scalaVersion := "2.12.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.2",
  "org.apache.spark" %% "spark-sql" % "3.1.2"
)
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.1.2" % "provided"

// https://mvnrepository.com/artifact/com.redislabs/spark-redis
libraryDependencies += "com.redislabs" %% "spark-redis" % "2.6.0"


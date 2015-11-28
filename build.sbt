name := "SparkStreamingStudy"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++=Seq(
	"org.apache.spark" % "spark-core_2.10" % "1.5.0",
	"org.apache.spark" % "spark-streaming_2.10" % "1.5.2",
	"org.apache.spark" % "spark-sql_2.10" % "1.5.2",
	"org.apache.spark" % "spark-streaming-kafka_2.10" % "1.5.2",
	"org.apache.spark" % "spark-mllib_2.10" % "1.5.2",
	"org.apache.kafka" % "kafka_2.10" % "0.8.2.2"
)
    
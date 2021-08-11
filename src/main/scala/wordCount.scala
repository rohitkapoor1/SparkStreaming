import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming._

object wordCount extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val runLocal = args(0).equals("l")
  val sparkConfig = new SparkConf()

  val spark = if(runLocal) {
    sparkConfig.set("spark.eventLog.enabled","true")
    sparkConfig.set("spark.master.bindAddress","localhost")
    sparkConfig.set("spark.eventLog.dir", "/Users/rkapoor/Desktop/spark-events")

    SparkSession
      .builder()
      .config(sparkConfig)
      .appName("word_count")
      .master("local[*]")
      .getOrCreate()
  }
  else {
    sparkConfig.set("spark.eventLog.enabled","true")
    SparkSession
      .builder()
      .config(sparkConfig)
      .appName("word_count")
      .getOrCreate()
  }

  val lines = spark
    .readStream
    .format("socket")
    .option("host","localhost")
    .option("port","9999")
    .load()

  import org.apache.spark.sql.functions._
  val words = lines
    .select(split(col("value"), " ").as("word"))
  val counts = words.groupBy("word").count()

  val checkpointDir = "/Users/rkapoor/Desktop/spark-streaming-events"
  val streamingQuery = counts.writeStream
    .format("console")
    .outputMode("complete")
    .trigger(Trigger.ProcessingTime("1 second"))
    .option("checkpointLocation", checkpointDir)
    .start()

//  println(streamingQuery.lastProgress.prettyJson)

  println("----------------------")
  println(streamingQuery.status.prettyJson)

  streamingQuery.awaitTermination()

}
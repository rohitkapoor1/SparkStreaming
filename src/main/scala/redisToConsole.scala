import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming._

object redisToConsole extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  val runLocal = args(0).equals("l")
  val sparkConfig = new SparkConf()

  val spark = if(runLocal) {
    sparkConfig.set("spark.eventLog.enabled","true")
    sparkConfig.set("spark.master.bindAddress", "localhost")
    sparkConfig.set("spark.eventLog.dir", "/Users/rkapoor/Desktop/spark-events")
    sparkConfig.set("spark.redis.host","localhost")
    sparkConfig.set("spark.redis.port","6379")

    SparkSession
      .builder()
      .master("local[*]")
      .config(sparkConfig)
      .appName("redisToConsole")
      .getOrCreate()
  }
  else {
    sparkConfig.set("spark.eventLog.enabled","true")
    SparkSession
      .builder()
      .config(sparkConfig)
      .master("yarn")
      .appName("redisToConsole")
      .getOrCreate()
  }

  val schema = "asset String, cost Long"
  val clicks = spark
    .readStream
    .format("redis")
    .option("stream.keys","clicks")
    .schema(schema)
    .load()

  val byAsset = clicks.groupBy("asset").count()

  // write stream to sink - here console
  val query = byAsset
    .writeStream
    .outputMode("update")
    .format("console")
    .start()

  query.awaitTermination()

  //spark.stop()

}

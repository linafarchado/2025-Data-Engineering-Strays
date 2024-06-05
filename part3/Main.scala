import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import spray.json._
import DefaultJsonProtocol._
import scala.sys.process._
import scala.util.Try
final case class DroneData(
  id: String,
  timestamp: Long,
  latitude: Double,
  longitude: Double,
  injuryIndex: Double,
  animalType: String
)

object DroneDataJsonProtocol extends DefaultJsonProtocol {
  implicit val droneDataFormat: RootJsonFormat[DroneData] = jsonFormat6(DroneData)
}

object KafkaSparkStructuredStreamingApp {
  import DroneDataJsonProtocol._

  def main(args: Array[String]): Unit = {
    // Create Spark configuration
    val conf = new SparkConf()
      .setAppName("Part3Strays")
      .setMaster("local[*]")

    // Create Spark Context
    val sc = SparkContext.getOrCreate(conf)

    // Create SparkSession
    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate()

    // Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")

    // Read data from Kafka
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "drone-data")
      .load()

    // Convert binary data to string
    import spark.implicits._
    val kafkaData = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]

    // Process each micro-batch as an RDD
    val query = kafkaData
      .writeStream
      .foreachBatch { (batchDS: Dataset[(String, String)], batchId: Long) =>
        // Execute hdfs dfs -ls command and parse the result to find the maximum batch ID
        // Execute hdfs dfs -ls command and parse the result to find the maximum batch ID
        val hdfsLsResult = "hdfs dfs -ls /user/alexandre/dronedata".!!
        val batchIds = hdfsLsResult
          .split("\n") // Split the result into lines
          .filter(_.contains("batch_")) // Filter lines containing batch_ to exclude other files
          .flatMap(_.split("/").last.split("\\.").headOption) // Extract the batch ID from the file names
          .flatMap(_.split("_").lift(1)) // Extract the ID part and convert it to Int
          .flatMap(id => Try(id.toInt).toOption) // Convert the ID part to Int, filtering out non-integer parts
        val maxBatchId = if (batchIds.nonEmpty) batchIds.max else 0 // Find the maximum batch ID, or set it to 0 if none found

        // Determine the next batch ID to use
        val nextBatchId = maxBatchId + 1

        // Extract the timestamps of the first and last records in the batch
        val timestamps = batchDS.map { case (_, jsonString) =>
          val data = jsonString.parseJson.convertTo[DroneData]
          data.timestamp
        }.collect()

        if (timestamps.nonEmpty) {
          val firstTimestamp = timestamps.min
          val lastTimestamp = timestamps.max

          // Convert Dataset to RDD
          val rdd = batchDS.rdd

          // Convert JSON strings to DroneData objects
          val droneDataRDD = rdd.map { case (_, jsonString) =>
            jsonString.parseJson.convertTo[DroneData]
          }

          // Convert RDD to Dataset
          val droneDataDS = spark.createDataset(droneDataRDD)

          // Define HDFS path for the batch
          val hdfsPath = s"hdfs://localhost:9000/user/alexandre/dronedata/batch_${nextBatchId}_${firstTimestamp}_${lastTimestamp}.parquet"

          // Write the batch in Parquet format
          droneDataDS.write.mode("append").parquet(hdfsPath)

          println(s"Batch stored in: $hdfsPath")
        } else {
          println("Skipping empty batch.")
        }
      }
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    // Await termination
    query.awaitTermination()
  }
}

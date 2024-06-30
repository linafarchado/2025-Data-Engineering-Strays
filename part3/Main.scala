import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import spray.json._
import DefaultJsonProtocol._
import java.net.InetAddress
import java.util.UUID
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
      .setMaster("local[*]") // Adjust this for cluster mode

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
      .option("kafka.group.id", "drone-data-group") // Define a consumer group id
      .option("startingOffsets", "latest") 
      .option("enable.auto.commit", "false") 
      .load()

    // Convert binary data to string
    import spark.implicits._
    val kafkaData = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]

    // Get the hostname of the machine
    val hostname = InetAddress.getLocalHost.getHostName

    // Generate a unique consumer ID for this instance
    val consumerId = s"$hostname-${UUID.randomUUID().toString}"

    // Process each micro-batch as an RDD
    val query = kafkaData
      .writeStream
      .foreachBatch { (batchDS: Dataset[(String, String)], batchId: Long) =>
        try {
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
            val hdfsPath = s"hdfs://localhost:9000/user/alexandre/dronedata/batch_${consumerId}_${firstTimestamp}_${lastTimestamp}.parquet"

            // Write the batch in Parquet format
            droneDataDS.write.mode("append").parquet(hdfsPath)

            println(s"Batch stored in: $hdfsPath")
          } else {
            println("Skipping empty batch.")
          }
        } catch {
          case e: Exception =>
            e.printStackTrace()
            println(s"Error processing batch $batchId: ${e.getMessage}")
        }
      }
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    // Await termination
    query.awaitTermination()
  }
}

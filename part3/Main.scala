import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import spray.json._
import DefaultJsonProtocol._

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
      .setAppName("Part2Strays")
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
        // Convert Dataset to RDD
        val rdd = batchDS.rdd

        // Convert JSON strings to DroneData objects
        val droneDataRDD = rdd.map { case (_, jsonString) =>
          jsonString.parseJson.convertTo[DroneData]
        }

        // Convert RDD to Dataset
        val droneDataDS = spark.createDataset(droneDataRDD)

        // Define HDFS path for the batch
        val hdfsPath = s"hdfs://localhost:9000/user/alexandre/dronedata/batch_$batchId.parquet"

        // Write the batch in Parquet format
        droneDataDS.write.mode("append").parquet(hdfsPath)
        
        println(s"Batch stored in: $hdfsPath")
      }
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    // Await termination
    query.awaitTermination()
  }
}

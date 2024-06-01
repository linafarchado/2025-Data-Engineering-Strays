import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import spray.json._
import DefaultJsonProtocol._
import sttp.client3._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

// Define case class to represent the JSON data
final case class DroneData(
  id: String,
  timestamp: Long,
  latitude: Double,
  longitude: Double,
  injuryIndex: Double,
  animalType: String
)

// Define JSON protocol for DroneData
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

    // Define Telegram bot credentials
    val telegramBotToken = "7283516832:AAF5OjWxIUkXwJI34tLQ_Neo4CFxX-Uva6I"
    val telegramChatId = "-4113094118"

    // Method to send a message to Telegram
    def sendTelegramMessage(message: String): Future[Unit] = {
      val request = basicRequest
        .get(uri"https://api.telegram.org/bot$telegramBotToken/sendMessage?chat_id=$telegramChatId&text=$message")
      Future {
        implicit val backend: SttpBackend[Identity, Any] = HttpURLConnectionBackend()
        request.send(backend)
        ()
      }
    }

    // Function to process each record in the RDD
    def processRecord(jsonString: String, batchId: Long): Unit = {
      val droneData = jsonString.parseJson.convertTo[DroneData]
      println(s"Batch: $batchId, ID: ${droneData.id}, Animal Type: ${droneData.animalType}, Injury Index: ${droneData.injuryIndex}")

      // Check injuryIndex and send Telegram notification if above 80
      if (droneData.injuryIndex > 80) {
        val message = s"High Injury Index Alert! \nAnimal Type: ${droneData.animalType}, Injury Index: ${droneData.injuryIndex}"
        sendTelegramMessage(message).recover {
          case ex: Exception => println(s"Failed to send Telegram message: ${ex.getMessage}")
        }
      }
    }

    // Process each micro-batch as an RDD
    val query = kafkaData
      .writeStream
      .foreachBatch { (batchDS: Dataset[(String, String)], batchId: Long) =>
        // Convert Dataset to RDD
        val rdd = batchDS.rdd

        // Process the RDD
        rdd.foreach { case (_, jsonString) =>
          processRecord(jsonString, batchId)
        }
      }
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("1 seconds"))
      .start()

    // Await termination
    query.awaitTermination()
  }
}

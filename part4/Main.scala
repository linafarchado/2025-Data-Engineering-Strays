import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets
import scala.concurrent.duration._

object DroneDataAnalyzer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DroneDataAnalyzer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val inputPath = "hdfs://localhost:9000/user/lina/dronedata/"

    val files = spark.sparkContext.textFile(inputPath).take(1)
    println(s"Files in directory: ${files.mkString(", ")}")

    val outputPath = "hdfs://localhost:9000/user/lina/analyzed_data/"

    def getLatestProcessedTimestamp(): Long = {
      val path = Paths.get("latest_processed_timestamp.txt")
      if (Files.exists(path)) {
        new String(Files.readAllBytes(path), StandardCharsets.UTF_8).toLong
      } else {
        0L
      }
    }

    def saveLatestProcessedTimestamp(timestamp: Long): Unit = {
      val path = Paths.get("latest_processed_timestamp.txt")
      Files.write(path, timestamp.toString.getBytes(StandardCharsets.UTF_8))
    }

    // Function to process new data
    def processNewData(latestTimestamp: Long): Unit = {
      val newData = spark.read.parquet(inputPath)
        .filter($"timestamp" > latestTimestamp)
        .cache()

      if (!newData.isEmpty) {
        val analyzedData = newData
          .groupBy("animalType")
          .agg(
            count("id").as("count"),
            avg("injuryIndex").as("avgInjuryIndex"),
            min("latitude").as("minLatitude"),
            max("latitude").as("maxLatitude"),
            min("longitude").as("minLongitude"),
            max("longitude").as("maxLongitude")
          )

        // Save analyzed data
        analyzedData.write.mode("append").parquet(outputPath)

        // Update latest processed timestamp
        val newLatestTimestamp = newData.agg(max("timestamp")).head().getLong(0)
        saveLatestProcessedTimestamp(newLatestTimestamp)

        println(s"Processed data up to timestamp: $newLatestTimestamp")
      } else {
        println("No new data to process.")
      }
    }

    // Main processing loop
    while (true) {
      val latestProcessedTimestamp = getLatestProcessedTimestamp()
      processNewData(latestProcessedTimestamp)
      Thread.sleep(30.seconds.toMillis)
    }
  }
}
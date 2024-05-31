import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.Dataset

object KafkaSparkStructuredStreamingApp {
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

        // Get the number of elements in the RDD
        val count = rdd.count()

        // Print the number of elements
        println(s"Batch $batchId has $count elements")
      }
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    // Await termination
    query.awaitTermination()
  }
}

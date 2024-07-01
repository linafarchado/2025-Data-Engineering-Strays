import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths}

object HDFSParquetReader {
  def main(args: Array[String]): Unit = {

    val userName = "lina"
    val filePath = s"hdfs://localhost:9000/user/$userName"

    // Create Spark configuration and Spark session
    val conf = new SparkConf().setAppName("HDFSParquetReader").setMaster("local[*]")
    val spark = SparkSession.builder.config(conf).getOrCreate()

    // Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")

    try {
      // Path to the Parquet file in HDFS
      val hdfsPath = s"$filePath/dronedata/*.parquet"

      // Read the Parquet file
      val parquetFileDF = spark.read.parquet(hdfsPath)

      // Animal type distribution
      val animalTypeDF = parquetFileDF.groupBy("animalType").count()
      val animaltype = s"$filePath/animaltype.csv"
      animalTypeDF.write.mode("append").option("header", "true").csv(animaltype)

      // Average injury index by animal type
      val avgInjuryByAnimalDF = parquetFileDF.groupBy("animalType")
        .agg(avg("injuryIndex").alias("avgInjuryIndex"))
        .orderBy(desc("avgInjuryIndex"))
      val avgInjury = s"$filePath/avgInjury.csv"
      avgInjuryByAnimalDF.write.mode("append").option("header", "true").csv(avgInjury)

      // Geographical analysis: incidents by quadrant
      val geoAnalysisDF = parquetFileDF.withColumn("quadrant",
        when(col("latitude") >= 0 && col("longitude") >= 0, "NE")
          .when(col("latitude") >= 0 && col("longitude") < 0, "NW")
          .when(col("latitude") < 0 && col("longitude") >= 0, "SE")
          .otherwise("SW")
      ).groupBy("quadrant").count().orderBy(desc("count"))
      val geoAnalysisPath = s"$filePath/geoAnalysis.csv"
      geoAnalysisDF.write.mode("append").option("header", "true").csv(geoAnalysisPath)

      // Tpop 3 animals by total injury index
      val topAnimalsByTotalInjuryDF = parquetFileDF.groupBy("animalType")
        .agg(sum("injuryIndex").alias("totalInjuryIndex"))
        .orderBy(desc("totalInjuryIndex"))
        .limit(3)
      val topAnimalsPath = s"$filePath/topAnimals.csv"
      topAnimalsByTotalInjuryDF.write.mode("append").option("header", "true").csv(topAnimalsPath)

      // Animal density by location
      val animalDensityDF = parquetFileDF
        .groupBy("latitude", "longitude", "animalType")
        .count()
        .orderBy(desc("count"))
      val animalDensityPath = s"$filePath/animalDensity.csv"
      animalDensityDF.write.mode("append").option("header", "true").csv(animalDensityPath)

      // Injury severity categories
      val injurySeverityDF = parquetFileDF
        .withColumn("severityCategory", when(col("injuryIndex") < 20, "Low")
          .when(col("injuryIndex").between(20, 50), "Medium")
          .when(col("injuryIndex").between(51, 80), "High")
          .otherwise("Critical"))
        .groupBy("severityCategory")
        .count()
      val injurySeverityPath = s"$filePath/injurySeverity.csv"
      injurySeverityDF.write.mode("append").option("header", "true").csv(injurySeverityPath)

      println("Files successfully written")
    } catch {
      case e: Exception =>
        e.printStackTrace()
        println("Error occurred: " + e.getMessage)
    } finally {
      // Stop the Spark session
      spark.stop()
    }
  }
}
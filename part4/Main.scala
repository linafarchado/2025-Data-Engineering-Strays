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

      // Injury index distribution
      val injuryStatsDF = parquetFileDF.select(
        mean("injuryIndex").alias("avgInjury"),
        stddev("injuryIndex").alias("stdDevInjury"),
        min("injuryIndex").alias("minInjury"),
        max("injuryIndex").alias("maxInjury")
      )
      val indexInjury = s"injuryIndex.csv"
      injuryStatsDF.write.mode("append").option("header", "true").csv(indexInjury)

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

      // Time-based analysis: incidents by hour of day
      val timeAnalysisDF = parquetFileDF.withColumn("hour", hour(from_unixtime(col("timestamp"))))
        .groupBy("hour")
        .count()
        .orderBy("hour")
      val timeAnalysisPath = s"$filePath/timeAnalysis.csv"
      timeAnalysisDF.write.mode("append").option("header", "true").csv(timeAnalysisPath)

      // High severity incidents
      val highSeverityDF = parquetFileDF.filter(col("injuryIndex") > 50)
        .select("id", "animalType", "injuryIndex")
        .orderBy(desc("injuryIndex"))
      val highSeverityPath = s"$filePath/highSeverity.csv"
      highSeverityDF.write.mode("append").option("header", "true").csv(highSeverityPath)

      // Tpop 3 animals by total injury index
      val topAnimalsByTotalInjuryDF = parquetFileDF.groupBy("animalType")
        .agg(sum("injuryIndex").alias("totalInjuryIndex"))
        .orderBy(desc("totalInjuryIndex"))
        .limit(3)
      val topAnimalsPath = s"$filePath/topAnimals.csv"
      topAnimalsByTotalInjuryDF.write.mode("append").option("header", "true").csv(topAnimalsPath)

      // Median Injury Index by Animal Type
      val medianInjuryByAnimalDF = parquetFileDF.groupBy("animalType")
        .agg(expr("percentile_approx(injuryIndex, 0.5)").alias("medianInjuryIndex"))
        .orderBy(desc("medianInjuryIndex"))
      val medianInjuryPath = s"$filePath/medianInjury.csv"
      medianInjuryByAnimalDF.write.mode("append").option("header", "true").csv(medianInjuryPath)

      // Incidents by Day of Week
      val incidentsByDayOfWeekDF = parquetFileDF.withColumn("dayOfWeek", date_format(from_unixtime(col("timestamp")), "EEEE"))
        .groupBy("dayOfWeek")
        .count()
        .orderBy("dayOfWeek")
      val incidentsByDayOfWeekPath = s"$filePath/incidentsByDayOfWeek.csv"
      incidentsByDayOfWeekDF.write.mode("append").option("header", "true").csv(incidentsByDayOfWeekPath)

      // Density of Incidents by Geographical Area (10x10 degree squares)
      val incidentsByGeoAreaDF = parquetFileDF.withColumn("latGroup", (col("latitude") / 10).cast("int"))
        .withColumn("lonGroup", (col("longitude") / 10).cast("int"))
        .groupBy("latGroup", "lonGroup")
        .count()
        .orderBy(desc("count"))
      val incidentsByGeoAreaPath = s"$filePath/incidentsByGeoArea.csv"
      incidentsByGeoAreaDF.write.mode("append").option("header", "true").csv(incidentsByGeoAreaPath)

      // Correlation between Injury Index and Latitude/Longitude
      val corrInjuryLatitude = parquetFileDF.stat.corr("injuryIndex", "latitude")
      val corrInjuryLongitude = parquetFileDF.stat.corr("injuryIndex", "longitude")
      val correlationsDF = spark.createDataFrame(Seq(
        ("injuryIndex_latitude", corrInjuryLatitude),
        ("injuryIndex_longitude", corrInjuryLongitude)
      )).toDF("correlationType", "value")

      val correlationsPath = s"$filePath/correlations.csv"
      correlationsDF.write.mode("append").option("header", "true").csv(correlationsPath)

      // Seasonal analysis
      val seasonalAnalysisDF = parquetFileDF
        .withColumn("month", month(from_unixtime(col("timestamp"))))
        .withColumn("season", when(col("month").isin(12, 1, 2), "Winter")
          .when(col("month").isin(3, 4, 5), "Spring")
          .when(col("month").isin(6, 7, 8), "Summer")
          .otherwise("Fall"))
        .groupBy("season")
        .agg(count("*").alias("count"), avg("injuryIndex").alias("avgInjuryIndex"))
      val seasonalAnalysisPath = s"$filePath/seasonalAnalysis.csv"
      seasonalAnalysisDF.write.mode("append").option("header", "true").csv(seasonalAnalysisPath)

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

      // Location data (assuming US states, adjust as needed)
      val locationDataDF = parquetFileDF
        .withColumn("state", when(col("latitude") > 40, "North").otherwise("South"))
        .groupBy("state")
        .agg(count("*").alias("incident_count"))
      val locationDataPath = s"$filePath/locationData.csv"
      locationDataDF.write.mode("append").option("header", "true").csv(locationDataPath)

      // Seasonal data with year
      val seasonalDataDF = parquetFileDF
        .withColumn("year", year(from_unixtime(col("timestamp"))))
        .withColumn("month", month(from_unixtime(col("timestamp"))))
        .groupBy("year", "month")
        .agg(count("*").alias("incident_count"))
        .orderBy("year", "month")
      val seasonalDataPath = s"$filePath/seasonalData.csv"
      seasonalDataDF.write.mode("append").option("header", "true").csv(seasonalDataPath)

      // Hourly and daily data
      val hourlyDailyDataDF = parquetFileDF
        .withColumn("hour", hour(from_unixtime(col("timestamp"))))
        .withColumn("day_of_week", date_format(from_unixtime(col("timestamp")), "EEEE"))
        .groupBy("hour", "day_of_week")
        .agg(count("*").alias("incident_count"))
      val hourlyDailyDataPath = s"$filePath/hourlyDailyData.csv"
      hourlyDailyDataDF.write.mode("append").option("header", "true").csv(hourlyDailyDataPath)

      // Animal type by season
      val animalSeasonDF = parquetFileDF
        .withColumn("month", month(from_unixtime(col("timestamp"))))
        .withColumn("season", when(col("month").isin(12, 1, 2), "Winter")
          .when(col("month").isin(3, 4, 5), "Spring")
          .when(col("month").isin(6, 7, 8), "Summer")
          .otherwise("Fall"))
        .groupBy("season", "animalType")
        .agg(count("*").alias("incident_count"))
      val animalSeasonPath = s"$filePath/animalSeasonData.csv"
      animalSeasonDF.write.mode("append").option("header", "true").csv(animalSeasonPath)

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
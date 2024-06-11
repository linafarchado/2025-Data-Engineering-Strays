import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths, Path}
import scala.collection.JavaConverters._

object DroneDataAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Drone Data Analysis")
      .master("local[*]")
      .getOrCreate()

    val baseDir = "/home/lina/studies/dataengineer/2025-Data-Engineering-Strays/part3/data"

    // Fonction pour récupérer les fichiers .parquet dans les sous-dossiers
    def getParquetFiles(dir: String): Array[String] = {
      Files.walk(Paths.get(dir))
        .iterator().asScala
        .filter(path => Files.isDirectory(path) && path.toString.endsWith(".parquet"))
        .flatMap { dirPath =>
          Files.list(dirPath).iterator().asScala
            .filter(filePath => filePath.toString.endsWith(".parquet"))
        }
        .map(_.toString)
        .toArray
    }

    // Récupérer tous les fichiers .parquet dans les sous-dossiers
    val parquetFiles = getParquetFiles(baseDir)

    // Lire les fichiers Parquet
    val df = spark.read.parquet(parquetFiles: _*)
    df.show(50)

    // Analyser les données
    val analysis = df.groupBy("animalType")
      .agg(
        count("*").as("count"),
        avg("injuryIndex").as("avgInjuryIndex"),
        max("injuryIndex").as("maxInjuryIndex")
      )

    // Sauvegarder les résultats en CSV
    analysis.write.mode("overwrite").csv("/home/lina/studies/dataengineer/2025-Data-Engineering-Strays/part3/analysis_result.csv")

    spark.stop()
  }
}
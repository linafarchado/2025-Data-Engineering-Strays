// src/main/scala/Main.scala
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties
import spray.json._
import DefaultJsonProtocol._
import scala.util.Random
import java.time.Instant

final case class DroneData(
                            id: String,
                            timestamp: Long,
                            latitude: Double,
                            longitude: Double,
                            injuryIndex: Double,
                            animalType: String
                          )

object DroneDataSimulator {
  val animalTypes = List("Cat", "Dog", "Bird", "Rabbit")

  def generateRandomData(): DroneData = {
    val id = java.util.UUID.randomUUID().toString
    val timestamp = Instant.now().getEpochSecond
    val latitude = Random.between(-90.0, 90.0)
    val longitude = Random.between(-180.0, 180.0)
    val injuryIndex = Random.between(0.0, 100.0)
    val animalType = Random.shuffle(animalTypes).head

    DroneData(id, timestamp, latitude, longitude, injuryIndex, animalType)
  }
}

object DroneDataJsonProtocol extends DefaultJsonProtocol {
  implicit val droneDataFormat: RootJsonFormat[DroneData] = jsonFormat6(DroneData.apply)
}

object Main {
  import DroneDataJsonProtocol._

  def main(args: Array[String]): Unit = {
    val topic = "drone-data"

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](props)

    lazy val simulateDataEmission: Unit = {
      val data = DroneDataSimulator.generateRandomData()
      val record = new ProducerRecord[String, String](topic, data.id, data.toJson.toString)
      producer.send(record)
      println(s"Sending data: ${data.toJson}")

      Thread.sleep(1000)
      simulateDataEmission
    }

    simulateDataEmission
  }
}
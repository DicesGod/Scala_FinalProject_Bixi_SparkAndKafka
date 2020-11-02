package ca.mcit.input.trip

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}

import scala.io.Source

object TripProducer extends App {
  def subscribeTrip(): Unit = {
    val topicName = "fall2019_minhle_trip"

    val producerProperties = new Properties()
    producerProperties.setProperty(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
    )
    producerProperties.setProperty(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName
    )
    producerProperties.setProperty(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName
    )

    val producer = new KafkaProducer[Int, String](producerProperties)

    val src = Source.fromFile("/Users/minhle/Downloads/bixi/100_trips.csv")
    val string1 = src.getLines()

    println("Produce data:")
    (0 to 10).foreach( _ =>
    string1.take(10).foreach { message =>
      producer.send(new ProducerRecord[Int, String](topicName, 1, message))
        }
      )
      producer.flush()
    }
}

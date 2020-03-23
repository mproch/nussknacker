package pl.touk.nussknacker.engine.performancetests.kafka

import java.io.File

import com.typesafe.scalalogging.LazyLogging
import io.circe.Encoder
import org.apache.kafka.clients.producer.ProducerRecord
import pl.touk.nussknacker.engine.flink.util.metrics.InstantRateMeter
import pl.touk.nussknacker.engine.kafka.{KafkaUtils, KafkaZookeeperServer}
import pl.touk.nussknacker.engine.process.performance.Model

object KafkaSender extends App with LazyLogging {

  val server = KafkaZookeeperServer.run(2181, 9092, Map.empty, new File("/tmp/kafkaTest"))

  val producer = KafkaUtils.createRawKafkaProducer(server.kafkaAddress, "test")

  val size = 50 * 1000000

  val topic = "testTopic"

  lazy val meter = new InstantRateMeter

  val codec = Encoder[Model]
  (1 to size).foreach { number =>
    val model = Model(s"record$number", number * 13)
    producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, number.toString.getBytes, codec(model).noSpaces.getBytes))
    meter.mark()
    if (number % 100000 == 0) {
      logger.info(s"Generated $number, rate: ${meter.getValue}")
    }
  }

  producer.flush()
  producer.close()
  server.shutdown()

}

object KafkaServer extends App {

  val server = KafkaZookeeperServer.run(2181, 9092, Map.empty, new File("/tmp/kafkaTest"))


}
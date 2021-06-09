package pl.touk.nussknacker.engine.kafka

import org.scalatest.{FunSuite, Matchers}

class KafkaSourceFactoryValidationSpec extends FunSuite with Matchers {

  private val modelData = LocalModel

  private def validator = ProcessValidator.default(definitions, new SimpleDictRegistry(Map.empty)).validate(process)



  test("should handle null topic") {

    val process = EspProcessBuilder.

  }

  test("should handle topic with incorrect expression") {

  }

}

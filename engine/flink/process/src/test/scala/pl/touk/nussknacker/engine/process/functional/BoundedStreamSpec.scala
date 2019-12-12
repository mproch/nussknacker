package pl.touk.nussknacker.engine.process.functional

import java.util.Date

import org.scalatest.{FunSuite, Matchers}
import pl.touk.nussknacker.engine.build.EspProcessBuilder
import pl.touk.nussknacker.engine.process.ProcessTestHelpers.{MockService, SimpleRecord, SimpleRecordWithPreviousValue, processInvoker}
import pl.touk.nussknacker.engine.spel

class BoundedStreamSpec extends FunSuite with Matchers {

  import spel.Implicits._

  test("sorts bounded") {
    val process = EspProcessBuilder.id("proc1")
      .exceptionHandler()
      .source("id", "input")
      .customNodeNoOutput("custom", "customSort", "sortBy" -> "#input.id")
      .processor("proc2", "logService", "all" -> "#input")
      .emptySink("out", "monitor")

    val data = List(
      SimpleRecord("5", 3, "a", new Date(0)),
      SimpleRecord("4", 5, "b", new Date(1000)),
      SimpleRecord("3", 12, "d", new Date(4000)),
      SimpleRecord("2", 14, "d", new Date(10000)),
      SimpleRecord("1", 20, "d", new Date(10000))

    )

    processInvoker.invoke(process, data)

    val mockData = MockService.data.map(_.asInstanceOf[SimpleRecord])
    mockData.map(_.id) shouldBe List("1", "2", "3", "4", "5")
  }

}

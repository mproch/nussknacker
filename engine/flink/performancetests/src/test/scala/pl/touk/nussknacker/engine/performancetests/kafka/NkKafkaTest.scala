package pl.touk.nussknacker.engine.performancetests.kafka

import java.util.UUID

import com.typesafe.config.ConfigFactory
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import pl.touk.nussknacker.engine.api.ProcessVersion
import pl.touk.nussknacker.engine.build.EspProcessBuilder
import pl.touk.nussknacker.engine.performancetests.PerformanceConfigCreator
import pl.touk.nussknacker.engine.process.FlinkStreamingProcessRegistrar
import pl.touk.nussknacker.engine.process.compiler.FlinkProcessCompiler
import pl.touk.nussknacker.engine.spel.Implicits._
import pl.touk.nussknacker.engine.testing.LocalModelData


object NkKafkaTest extends App {

  val process = EspProcessBuilder
    .id(s"test-${UUID.randomUUID().toString}".replaceAll("-", "_"))
    .parallelism(1)
    .exceptionHandlerNoParams()
    .source("start", "kafkaSource")
    //.filter("f1", "#input.field2() % 1000000 == 0")
    .filter("f1", "#input.field2() % 1000000 == 0L")
    //.filter("filter", "{#input}.?[#this.field2 % 1000000 != 0].isEmpty()")
    .sink("end", "#input", "sink")

  val env = StreamExecutionEnvironment.createLocalEnvironment()

  val modelData = LocalModelData(ConfigFactory.empty(), PerformanceConfigCreator)
  val compiler = new FlinkProcessCompiler(modelData)
  val registrar = FlinkStreamingProcessRegistrar(compiler, modelData.processConfig)
  registrar.register(env, process, ProcessVersion.empty)
  val start = System.currentTimeMillis()
  env.execute(process.id)
  println(s"Total run time: ${System.currentTimeMillis() - start}")

}

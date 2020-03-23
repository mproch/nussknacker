package pl.touk.nussknacker.engine.performancetests

import com.sksamuel.avro4s
import com.sksamuel.avro4s.AvroInputStream
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.avro.Schema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import pl.touk.nussknacker.engine.api.{CirceUtil, ProcessVersion}
import pl.touk.nussknacker.engine.api.exception.ExceptionHandlerFactory
import pl.touk.nussknacker.engine.api.process.{ExpressionConfig, SinkFactory, SourceFactory, WithCategories}
import pl.touk.nussknacker.engine.build.EspProcessBuilder
import pl.touk.nussknacker.engine.flink.util.exception.BrieflyLoggingExceptionHandler
import pl.touk.nussknacker.engine.kafka.generic.sources.JsonDecoderDeserialization
import pl.touk.nussknacker.engine.kafka.{KafkaConfig, SingleTopicKafkaSourceFactory}
import pl.touk.nussknacker.engine.process.FlinkStreamingProcessRegistrar
import pl.touk.nussknacker.engine.process.compiler.FlinkProcessCompiler
import pl.touk.nussknacker.engine.process.performance.{GeneratingSourceFactory, Model, WriteSink}
import pl.touk.nussknacker.engine.spel.Implicits._
import pl.touk.nussknacker.engine.testing.{EmptyProcessConfigCreator, LocalModelData}
import pl.touk.nussknacker.engine.util.functions.{date, geo, numeric}
import org.apache.flink.api.scala._
import pl.touk.nussknacker.engine.api.test.{SimpleTestDataSplit, TestParsingUtils}
import pl.touk.nussknacker.engine.flink.util.source.EspDeserializationSchema


object PerformanceConfigCreator extends EmptyProcessConfigCreator {

  override def sourceFactories(config: Config): Map[String, WithCategories[SourceFactory[_]]] = {
    Map("source" -> WithCategories(GeneratingSourceFactory),
      "kafkaSource" -> WithCategories(
      new SingleTopicKafkaSourceFactory[Model]("testTopic", KafkaConfig("localhost:9092", Some(Map(
        "auto.offset.reset" -> "earliest"
      )), None),
        new JsonDecoderDeserialization[Model], None, TestParsingUtils.newLineSplit
      ))
    )
  }

  override def sinkFactories(config: Config): Map[String, WithCategories[SinkFactory]] = {
    Map("sink" -> WithCategories(SinkFactory.noParam(WriteSink)))
  }

  override def expressionConfig(config: Config): ExpressionConfig = {
    ExpressionConfig(
      Map(
        "GEO" -> WithCategories(geo),
        "NUMERIC" -> WithCategories(numeric),
        "DATE" -> WithCategories(date)
      ),
      List(),
      hideMetaVariable = true
    )
  }

  override def exceptionHandlerFactory(config: Config): ExceptionHandlerFactory = ExceptionHandlerFactory.noParams(BrieflyLoggingExceptionHandler(_))
}

object NkBaseTest extends App {

  val process = EspProcessBuilder
    .id("test")
    .parallelism(1)
    .exceptionHandlerNoParams()
    .source("start", "source", "size" -> "50 * 1000000")
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

class RawAvroDecoderDeserialization[T:avro4s.Decoder:TypeInformation](schema: Schema) extends EspDeserializationSchema[T](ba => {
  val input = AvroInputStream.binary[T].from(ba).build(schema)
  val result = input.iterator.next()
  input.close()
  result
})

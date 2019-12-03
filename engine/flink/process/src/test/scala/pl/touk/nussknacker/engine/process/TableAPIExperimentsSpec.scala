package pl.touk.nussknacker.engine.process

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import org.scalatest.{FunSuite, Matchers}
import pl.touk.nussknacker.engine.api.lazyy.LazyContext
import pl.touk.nussknacker.engine.api.{Context, CustomStreamTransformer, MethodToInvoke, ProcessVersion, ValueWithContext}
import pl.touk.nussknacker.engine.flink.api.process.{FlinkCustomNodeContext, FlinkCustomStreamTransformation, FlinkSink, FlinkSourceFactory}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.{PrintSinkFunction, SinkFunction}
import pl.touk.nussknacker.engine.api.exception.ExceptionHandlerFactory
import pl.touk.nussknacker.engine.api.process.{SinkFactory, SourceFactory, WithCategories}
import pl.touk.nussknacker.engine.api.typed.typing.Typed
import pl.touk.nussknacker.engine.build.EspProcessBuilder
import pl.touk.nussknacker.engine.flink.test.{FlinkTestConfiguration, StoppableExecutionEnvironment}
import pl.touk.nussknacker.engine.flink.util.exception.{BrieflyLoggingExceptionHandler, VerboselyLoggingExceptionHandler}
import pl.touk.nussknacker.engine.flink.util.source.CollectionSource
import pl.touk.nussknacker.engine.process.compiler.FlinkStreamingProcessCompiler
import pl.touk.nussknacker.engine.testing.{EmptyProcessConfigCreator, LocalModelData}
import pl.touk.nussknacker.engine.spel.Implicits._

import scala.collection.JavaConverters._

class TableAPIExperimentsSpec extends FunSuite with Matchers {

  ignore("should run??") {

    val modelData = LocalModelData(ConfigFactory.empty(), new SortConfigCreator)

    val stoppableEnv = StoppableExecutionEnvironment(FlinkTestConfiguration.configuration())

    val registrar = new FlinkStreamingProcessCompiler(modelData.configCreator, modelData.processConfig).createFlinkProcessRegistrar()


    val process = EspProcessBuilder
      .id("done")
      .exceptionHandler()
        .source("source", "source")
        .buildSimpleVariable("f1", "field1", "#input.f1")
        .buildSimpleVariable("f2", "field2", "#input.f2")

        .customNodeNoOutput("sort", "sort")
        .sink("sink", "#input", "result")

    registrar.register(new StreamExecutionEnvironment(stoppableEnv), process, ProcessVersion.empty)

    stoppableEnv.withJobRunning("done") {
      Thread.sleep(3000)
    }

  }


}

class SortConfigCreator extends EmptyProcessConfigCreator {

  override def exceptionHandlerFactory(config: Config): ExceptionHandlerFactory
  = ExceptionHandlerFactory.noParams(VerboselyLoggingExceptionHandler(_))

  override def customStreamTransformers(config: Config): Map[String, WithCategories[CustomStreamTransformer]] = Map("sort" -> WithCategories(TotalSortCreator))

  override def sourceFactories(config: Config): Map[String, WithCategories[SourceFactory[_]]] = Map("source" ->
    WithCategories(FlinkSourceFactory.noParam(new CollectionSource[java.util.Map[String, String]](new ExecutionConfig, List(
      Map("f1" -> "1", "f2" -> "a").asJava,
      Map("f1" -> "3", "f2" -> "c").asJava,
      Map("f1" -> "5", "f2" -> "e").asJava,
      Map("f1" -> "2", "f2" -> "b").asJava,
      Map("f1" -> "4", "f2" -> "d").asJava
    ), None, Typed[java.util.Map[_, _]])))
  )

  override def sinkFactories(config: Config): Map[String, WithCategories[SinkFactory]] = {
    Map("result" -> WithCategories(SinkFactory.noParam(new FlinkSink {
      override def toFlinkFunction: SinkFunction[Any] = new PrintSinkFunction

      override def testDataOutput: Option[Any => String] = None
    })))
  }
}

object TotalSortCreator extends CustomStreamTransformer {

  @MethodToInvoke(returnType = classOf[Void])
  def invoke() = new TotalSort

}

class TotalSort extends FlinkCustomStreamTransformation {

  override def transform(start: DataStream[Context], context: FlinkCustomNodeContext): DataStream[ValueWithContext[Any]] = {

    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val bsTableEnv = StreamTableEnvironment.create(start.executionEnvironment, bsSettings)


    val datatype = new RowTypeInfo(Array[TypeInformation[_]](BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO), Array("field1", "field2"))

    val ordered = bsTableEnv
      .fromDataStream(start.map(ctx => Row.of(ctx.apply("field1"), ctx.apply("field2")))(datatype))
      //.orderBy("field1")

    bsTableEnv.toAppendStream[Row](ordered)(datatype)
      .map(row => ValueWithContext[Any](null, Context("id", Map("input" ->
        Map("field1" -> row.getField(0), "field2" -> row.getField(1))), LazyContext("id"), None)))
  }
}

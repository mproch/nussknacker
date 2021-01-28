package pl.touk.nussknacker.engine.process.performance

import java.io.{File, PrintWriter}
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValueFactory.fromAnyRef
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import pl.touk.nussknacker.engine.api.exception.ExceptionHandlerFactory
import pl.touk.nussknacker.engine.api.process._
import pl.touk.nussknacker.engine.api.typed.typing.{Typed, TypingResult}
import pl.touk.nussknacker.engine.api.typed.{ReturningType, typing}
import pl.touk.nussknacker.engine.api.{CustomStreamTransformer, MethodToInvoke, ProcessVersion, Service, ValueWithContext}
import pl.touk.nussknacker.engine.build.EspProcessBuilder
import pl.touk.nussknacker.engine.flink.api.process.{FlinkCustomNodeContext, FlinkCustomStreamTransformation, FlinkSource}
import pl.touk.nussknacker.engine.flink.api.timestampwatermark.TimestampWatermarkHandler
import pl.touk.nussknacker.engine.flink.util.exception.BrieflyLoggingExceptionHandler
import pl.touk.nussknacker.engine.flink.util.sink.EmptySink
import pl.touk.nussknacker.engine.graph.EspProcess
import pl.touk.nussknacker.engine.process.ExecutionConfigPreparer
import pl.touk.nussknacker.engine.process.compiler.FlinkProcessCompiler
import pl.touk.nussknacker.engine.process.registrar.FlinkProcessRegistrar
import pl.touk.nussknacker.engine.process.typeinformation.TypeInformationDetection
import pl.touk.nussknacker.engine.testing.LocalModelData
import pl.touk.nussknacker.engine.util.process.EmptyProcessConfigCreator
import pl.touk.nussknacker.engine.spel.Implicits._

import scala.concurrent.{ExecutionContext, Future}

object PerformanceCheck extends App {

  val process = EspProcessBuilder
    .id("test")
    .exceptionHandler()
    .source("source", "source")
    //.buildSimpleVariable("v1", "v1", s"{${(1 to 20).map(i => s"v$i: $i").mkString(", ")}}")
    //.customNodeNoOutput("custom", "transform")
    .processor("counter", "counter")
    .emptySink("sink", "sink")

  new PerformanceCheck(
    sourceFactory = mapFactory,
    testDataCount = 5000000,
    async = true,
    useRealEnricher = false,
    useTypingResultTypeInformation = true
  ).run(process)

  def stringFactory(path: String) = new FileSourceFactory[String](path,
    identity[String],
    TypeInformation.of(classOf[String]), Typed[String])
  def mapFactory(path: String) = new FileSourceFactory[java.util.Map[String, String]](path,
    prepare, TypeInformation.of(classOf[java.util.Map[String, String]]), Typed.fromInstance(prepare("")))

  def prepare(k: String) = {
    val s = new java.util.HashMap[String, String]
    (0 to 20).foreach(i => s.put("v" + i, k))
    s
  }

}


class PerformanceCheck(sourceFactory: String => SourceFactory[_],
              testDataCount: Int,
              useRealEnricher: Boolean,
              useTypingResultTypeInformation: Boolean,
              async: Boolean
             ) {

  private def generateTestData(count: Int): File = {
    val file = Files.createTempFile("test1", ".csv").toFile
    file.deleteOnExit()

    val fis = new PrintWriter(file)
    try {
      (1 to count).foreach { id =>
        fis.println(s"test$id")
      }
    } finally {
      fis.close()
    }
    file
  }

  def run(process: EspProcess): Unit = {
    val path = generateTestData(testDataCount)
    println("generated data")
    val pc = new Creator(sourceFactory(path.getAbsolutePath), useRealEnricher)
    val env = StreamExecutionEnvironment.createLocalEnvironment(1)
    val config = ConfigFactory.empty()
      .withValue("asyncExecutionConfig.defaultUseAsyncInterpretation", fromAnyRef(async))
      .withValue("globalParameters.useTypingResultTypeInformation", fromAnyRef(useTypingResultTypeInformation))
    val modelData = LocalModelData(config, pc)
    FlinkProcessRegistrar(new FlinkProcessCompiler(modelData), config, ExecutionConfigPreparer.defaultChain(modelData))
      .register(env, process, ProcessVersion.empty)
    env.execute()
  }

}

class Creator(sourceToUse: SourceFactory[_], useRealEnricher: Boolean) extends EmptyProcessConfigCreator {
  override def services(processObjectDependencies: ProcessObjectDependencies): Map[String, WithCategories[Service]] = {
    Map("counter" -> WithCategories(new Counter(useRealEnricher)))
  }

  override def sourceFactories(processObjectDependencies: ProcessObjectDependencies): Map[String, WithCategories[SourceFactory[_]]] = {
    Map("source" -> WithCategories(sourceToUse))
  }

  override def sinkFactories(processObjectDependencies: ProcessObjectDependencies): Map[String, WithCategories[SinkFactory]] = {
    Map("sink" -> WithCategories(SinkFactory.noParam(EmptySink)))
  }

  override def customStreamTransformers(processObjectDependencies: ProcessObjectDependencies): Map[String, WithCategories[CustomStreamTransformer]] = {
    Map("transform" -> WithCategories(Transform))
  }

  override def exceptionHandlerFactory(processObjectDependencies: ProcessObjectDependencies): ExceptionHandlerFactory = {
    ExceptionHandlerFactory.noParams(m => BrieflyLoggingExceptionHandler(m, Map.empty))
  }
}

class Counter(realEnricher: Boolean) extends Service {

  private lazy val start = System.currentTimeMillis()
  private val counter = new AtomicLong(0)

  @MethodToInvoke
  def run()(implicit ec: ExecutionContext): Future[Unit] = {
    if (counter.getAndIncrement() % 10000 == 0) {
      val totalTime = math.max(1, System.currentTimeMillis() - start)
      println(s"${counter.get()} in ${totalTime}ms (${1000 * counter.get() / totalTime}/s)")
    }
    if (realEnricher) {
      Future {
        Thread.sleep(0, 500)
      }
    } else {
      Future.successful(())
    }
  }

  override def close(): Unit = {
    val totalTime = System.currentTimeMillis() - start
    println(s"Finished ${counter.get()} in ${totalTime}ms (${1000 * counter.get() / totalTime}/s)")
  }
}

object Transform extends CustomStreamTransformer {

  @MethodToInvoke(returnType = classOf[Void])
  def invoke(): FlinkCustomStreamTransformation = {
    FlinkCustomStreamTransformation((ds, cc) => {
      val ti = TypeInformationDetection.forExecutionConfig(ds.executionEnvironment.getConfig, getClass.getClassLoader)
        .forValueWithContext[AnyRef](cc.validationContext.left.get, Typed[String])
      ds
        .keyBy(_ => "")(TypeInformation.of(classOf[String]))
        .map(c => ValueWithContext[AnyRef](null, c))(ti)
    })
  }

}

class FileSourceFactory[T](path: String,
                           parser: String => T,
                           ti: TypeInformation[T],
                           tr: TypingResult) extends SourceFactory[T] {

  @MethodToInvoke
  def create(): Source[T] = {

    new FlinkSource[T] with ReturningType {
      override def sourceStream(env: StreamExecutionEnvironment, flinkNodeContext: FlinkCustomNodeContext): DataStream[T] = {
        env
          .readTextFile(path)
          .map(parser)(typeInformation)
      }

      override def timestampAssignerForTest: Option[TimestampWatermarkHandler[T]] = None

      override def typeInformation: TypeInformation[T] = ti

      override def returnType: typing.TypingResult = tr
    }
  }

  override def clazz: Class[_] = classOf[Any]
}
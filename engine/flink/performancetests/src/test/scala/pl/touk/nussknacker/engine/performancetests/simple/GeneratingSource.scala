package pl.touk.nussknacker.engine.process.performance

import com.codahale.metrics.Meter
import com.typesafe.scalalogging.LazyLogging
import io.circe.generic.JsonCodec
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.TimestampAssigner
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import pl.touk.nussknacker.engine.api.{MethodToInvoke, ParamName}
import pl.touk.nussknacker.engine.flink.api.process.{BasicFlinkSource, FlinkSink, FlinkSource, FlinkSourceFactory}
import pl.touk.nussknacker.engine.flink.util.metrics.InstantRateMeter

class GeneratingSource(size: Int) extends SourceFunction[Model] with LazyLogging {

  @volatile var running = true

  @volatile var start = 0L

  lazy val meter = new InstantRateMeter

  override def run(ctx: SourceFunction.SourceContext[Model]): Unit = {
    start = System.currentTimeMillis()
    (1 to size).foreach { number =>
      ctx.collect(Model(s"record$number", number * 13))
      meter.mark()
      if (number % 100000 == 0) {
        logger.info(s"Generated $number, rate: ${meter.getValue}")
      }
    }
  }

  override def cancel(): Unit = {
    running = false
    logger.info(s"Took: ${System.currentTimeMillis() - start}")
  }
}

object WriteSinkFunction extends SinkFunction[Any] with LazyLogging {

  override def invoke(value: Any, context: SinkFunction.Context[_]): Unit = {
    logger.info(s"Result: $value")
  }
}

object WriteSink extends FlinkSink {
  override def toFlinkFunction: SinkFunction[Any] = WriteSinkFunction

  override def testDataOutput: Option[Any => String] = None
}

object GeneratingSourceFactory extends FlinkSourceFactory[Model] {

  @MethodToInvoke
  def run(@ParamName("size") size: Int): FlinkSource[Model] = {

    new BasicFlinkSource[Model] {
      override def flinkSourceFunction: SourceFunction[Model] = new GeneratingSource(size)

      override def typeInformation: TypeInformation[Model] = implicitly[TypeInformation[Model]]

      override def timestampAssigner: Option[TimestampAssigner[Model]] = None
    }
  }

}

@JsonCodec case class Model(field1: String, field2: Long)
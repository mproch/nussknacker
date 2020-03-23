package pl.touk.nussknacker.engine.performancetests.simple

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import pl.touk.nussknacker.engine.process.performance.GeneratingSource
import org.apache.flink.api.scala._

object BaseFlinkTest extends App {

  val size = 50 * 1000000

  val env = StreamExecutionEnvironment.createLocalEnvironment(1)
  env.getConfig.enableObjectReuse()
  env
    .addSource(new GeneratingSource(size))
    .filter(_.field2 % 1000000 == 0)
    .print()

  println(env.execute("run").getNetRuntime)
}


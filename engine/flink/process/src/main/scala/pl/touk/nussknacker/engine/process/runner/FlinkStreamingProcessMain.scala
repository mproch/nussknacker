package pl.touk.nussknacker.engine.process.runner

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import pl.touk.nussknacker.engine.ModelData
import pl.touk.nussknacker.engine.api.ProcessVersion
import pl.touk.nussknacker.engine.flink.api.NKGlobalParameters
import pl.touk.nussknacker.engine.graph.EspProcess
import pl.touk.nussknacker.engine.process.FlinkStreamingProcessRegistrar
import pl.touk.nussknacker.engine.process.compiler.FlinkProcessCompiler
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._

object FlinkStreamingProcessMain extends FlinkProcessMain[StreamExecutionEnvironment] {

  override protected def getExecutionEnvironment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  override protected def getConfig(env: StreamExecutionEnvironment): ExecutionConfig = env.getConfig

  override protected def runProcess(env: StreamExecutionEnvironment,

                                    modelData: ModelData,
                                    process: EspProcess,
                                    processVersion: ProcessVersion): Unit = {
    val compiler = new FlinkProcessCompiler(modelData)
    val registrar = FlinkStreamingProcessRegistrar(compiler, modelData.processConfig)
    registrar.register(env, process, processVersion)
    env.execute(process.id)
  }
}
package pl.touk.nussknacker.openapi

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import io.circe.syntax.EncoderOps
import net.ceedubs.ficus.Ficus._
import org.apache.commons.io.IOUtils
import pl.touk.nussknacker.engine.api.CirceUtil
import pl.touk.nussknacker.engine.api.component.{ComponentDefinition, ComponentProvider, NussknackerVersion}
import pl.touk.nussknacker.engine.api.process.ProcessObjectDependencies
import pl.touk.nussknacker.engine.util.config.ConfigEnrichments._
import pl.touk.nussknacker.openapi.OpenAPIsConfig._
import pl.touk.nussknacker.openapi.enrichers.{BaseSwaggerEnricherCreator, SwaggerEnrichers}
import pl.touk.nussknacker.openapi.http.backend.{DefaultHttpClientConfig, HttpClientConfig}
import pl.touk.nussknacker.openapi.parser.SwaggerParser

import scala.jdk.CollectionConverters.seqAsJavaListConverter

class OpenAPIComponentProvider extends ComponentProvider with LazyLogging {

  override def providerName: String = "openAPI"

  override def resolveConfigForExecution(config: Config): Config = {
    val openAPIsConfig = config.rootAs[OpenAPIServicesConfig]
    // Warning: openapi specification can be encoded in Unicode (UTF-8, UTF-16, UTF-32)
    val definition = IOUtils.toString(openAPIsConfig.url, "UTF-8")
    val services = SwaggerParser.parse(definition, openAPIsConfig.securities.getOrElse(Map.empty))
    logger.info(s"Discovered OpenAPI: ${services.map(_.name)}")

    val servicesConfig = services.map(service => ConfigFactory.parseString(service.asJson.spaces2).root())
    config.withValue("services", ConfigValueFactory.fromIterable(servicesConfig.asJava))
  }

  override def create(config: Config, dependencies: ProcessObjectDependencies): List[ComponentDefinition] = {
    val openAPIsConfig = config.rootAs[OpenAPIServicesConfig]
    val serviceDefinitionConfig = config.getList("services").render(ConfigRenderOptions.concise())
    val swaggerServices =
      CirceUtil.decodeJsonUnsafe[List[SwaggerService]](serviceDefinitionConfig, "Failed to parse service config")

    //TODO: configuration
    val fixedParameters: Map[String, () => AnyRef] = Map.empty
    new SwaggerEnrichers(openAPIsConfig.rootURL, prepareBaseEnricherCreator(config))
      .enrichers(swaggerServices, Nil, fixedParameters)
      //FIXME: documentation is ignore ATM
      .map(service => ComponentDefinition(service.name, service.service)).toList
  }

  protected def prepareBaseEnricherCreator(config: Config): BaseSwaggerEnricherCreator = {
    val clientConfig = config.getAs[HttpClientConfig]("httpClientConfig").getOrElse(DefaultHttpClientConfig())
    BaseSwaggerEnricherCreator(clientConfig)
  }

  override def isCompatible(version: NussknackerVersion): Boolean = true

}

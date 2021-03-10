package pl.touk.nussknacker.engine.api.deployment

import io.circe.generic.extras.semiauto.{deriveUnwrappedDecoder, deriveUnwrappedEncoder}
import io.circe.{Decoder, Encoder}

//id generated by external system - e.g. Flink JobID
final case class ExternalDeploymentId(value: String) extends AnyVal

object ExternalDeploymentId {
  implicit val encoder: Encoder[ExternalDeploymentId] = deriveUnwrappedEncoder
  implicit val decoder: Decoder[ExternalDeploymentId] = deriveUnwrappedDecoder
}
package pl.touk.nussknacker.engine.api.deployment

import io.circe.generic.JsonCodec
import io.circe.generic.extras.ConfiguredJsonCodec
import pl.touk.nussknacker.engine.api.definition.Parameter

//TODO: other params? Do we need label here?
@JsonCodec
case class CustomAction(name: String,
                       //TODO: parameters
                        parameters: List[String],
                       //or just deployment??
                        blocksOtherActions: Boolean,

                       )


@ConfiguredJsonCodec
sealed trait CustomActionParameter {
  def name: String
}

object ProcessJsonParameter extends CustomActionParameter {
  val name = "processJson"
}

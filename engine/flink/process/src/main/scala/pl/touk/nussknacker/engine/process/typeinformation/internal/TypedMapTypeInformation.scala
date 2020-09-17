package pl.touk.nussknacker.engine.process.typeinformation.internal

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import pl.touk.nussknacker.engine.util.Implicits._

import scala.collection.immutable.TreeMap

//TODO: handle other classes e.g. java.util.Map, avro...
case class TypedMapTypeInformation(informations: Map[String, TypeInformation[_]]) extends TypeInformation[Map[String, AnyRef]] {

  override def isBasicType: Boolean = false

  override def isTupleType: Boolean = false

  override def getArity: Int = 1

  override def getTotalFields: Int = 1

  override def getTypeClass: Class[Map[String, AnyRef]] = classOf[Map[String, AnyRef]]

  override def isKeyType: Boolean = false

  override def createSerializer(config: ExecutionConfig): TypeSerializer[Map[String, AnyRef]] =
    TypedMapSerializer((TreeMap[String, TypeSerializer[_]]() ++ informations.mapValuesNow(_.createSerializer(config))).toList)

  override def canEqual(obj: Any): Boolean = obj.isInstanceOf[MapBasedTypeInformation[_]]

}

case class TypedMapSerializer(serializers: List[(String, TypeSerializer[_])])

  extends MapBasedTypeSerializer[Map[String, AnyRef]](serializers) with LazyLogging {

  override def deserialize(values: Array[AnyRef]): Map[String, AnyRef] = names.zip(values).toMap

  override def get(value: Map[String, AnyRef], key: String): AnyRef = value.getOrElse(key, null)

  override def duplicate(serializers: List[(String, TypeSerializer[_])]): TypeSerializer[Map[String, AnyRef]] = TypedMapSerializer(serializers)

  override def createInstance(): Map[String, AnyRef] = Map.empty

  override def snapshotConfiguration(): TypeSerializerSnapshot[Map[String, AnyRef]] = new TypedMapSerializerSnapshot(serializers)
}

class TypedMapSerializerSnapshot extends MapBasedSerializerSnapshot[Map[String, AnyRef]] {

  def this(serializers: List[(String, TypeSerializer[_])]) = {
    this()
    this.serializers = serializers
  }

  override def restoreSerializer(): TypeSerializer[Map[String, AnyRef]] = TypedMapSerializer(serializers)
}
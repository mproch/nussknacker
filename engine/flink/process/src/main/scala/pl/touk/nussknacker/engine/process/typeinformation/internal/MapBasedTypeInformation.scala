package pl.touk.nussknacker.engine.process.typeinformation.internal

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.util.InstantiationUtil

import scala.reflect.ClassTag

//TODO: handle other classes e.g. java.util.Map, avro...
abstract class MapBasedTypeInformation[T:ClassTag](informations: List[(String, TypeInformation[_])]) extends TypeInformation[T] {

  override def isBasicType: Boolean = false

  override def isTupleType: Boolean = false

  override def getArity: Int = 1

  override def getTotalFields: Int = 1

  override def getTypeClass: Class[T] = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]

  override def isKeyType: Boolean = false

  override def createSerializer(config: ExecutionConfig): TypeSerializer[T] =
    createSerializer(informations.map {
      case (k, v) => (k, v.createSerializer(config))
    })

  override def canEqual(obj: Any): Boolean = obj.isInstanceOf[MapBasedTypeInformation[T]]

  def createSerializer(serializers: List[(String, TypeSerializer[_])]): TypeSerializer[T]
}

abstract class MapBasedTypeSerializer[T](serializers: List[(String, TypeSerializer[_])]) extends TypeSerializer[T] with LazyLogging {

  protected val names: Array[String] = serializers.map(_._1).toArray

  private val serializersInstances = serializers.map(_._2).toArray

  override def isImmutableType: Boolean = serializers.forall(_._2.isImmutableType)

  override def duplicate(): TypeSerializer[T] = duplicate(
    serializers.map {
      case (k, s) => (k, s.duplicate())
    })
  
  override def copy(from: T): T = from

  //???
  override def copy(from: T, reuse: T): T = copy(from)

  override def getLength: Int = -1

  override def serialize(record: T, target: DataOutputView): Unit = {
    serializers.foreach { case (key, serializer) =>
      val valueToSerialize = get(record, key)
      if (valueToSerialize == null) {
        target.writeBoolean(true)
      } else {
        target.writeBoolean(false)
        serializer.asInstanceOf[TypeSerializer[AnyRef]].serialize(valueToSerialize, target)
      }
    }
    /*if (mapped.keySet != serializers.map(_._1).toSet) {
      logger.warn(s"Different keys in TypedObject serialization, in record: ${mapped.keySet}, in definition: ${serializers.map(_._1)}")
    } */
  }

  override def deserialize(source: DataInputView): T = {
    val array: Array[AnyRef] = new Array[AnyRef](serializers.size)
    serializers.indices.foreach { idx =>
      array(idx) = if (!source.readBoolean()) {
        serializersInstances(idx).asInstanceOf[TypeSerializer[AnyRef]].deserialize(source)
      } else {
        null
      }
    }
    deserialize(array)
  }

  override def deserialize(reuse: T, source: DataInputView): T = deserialize(source)

  override def copy(source: DataInputView, target: DataOutputView): Unit = serializers.map(_._2).foreach(_.copy(source, target))

  def deserialize(values: Array[AnyRef]): T

  def get(value: T, name: String): AnyRef

  def duplicate(serializers: List[(String, TypeSerializer[_])]): TypeSerializer[T]
}

abstract class MapBasedSerializerSnapshot[T] extends TypeSerializerSnapshot[T] {

  protected var serializers: List[(String, TypeSerializer[_])] = _

  def this(serializers: List[(String, TypeSerializer[_])]) = {
    this()
    this.serializers = serializers
  }

  override def getCurrentVersion: Int = 0

  override def writeSnapshot(out: DataOutputView): Unit = {
    out.writeInt(serializers.size)
    serializers.foreach {
      case (k, v) =>
        out.writeUTF(k)
        val config = v.snapshotConfiguration()
        out.writeUTF(config.getClass.getCanonicalName)
        config.writeSnapshot(out)
    }
  }

  override def readSnapshot(readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit = {
    val size = in.readInt()
    serializers = (0 until size).map { _ =>
      val key = in.readUTF()
      val snapshot = InstantiationUtil.resolveClassByName(in, userCodeClassLoader)
        .getConstructor().newInstance().asInstanceOf[TypeSerializerSnapshot[_]]
      snapshot.readSnapshot(1, in, userCodeClassLoader)
      (key, snapshot.restoreSerializer())
    }.toList
  }

  //??
  override def resolveSchemaCompatibility(newSerializer: TypeSerializer[T]): TypeSerializerSchemaCompatibility[T]
  = TypeSerializerSchemaCompatibility.compatibleAsIs()

}
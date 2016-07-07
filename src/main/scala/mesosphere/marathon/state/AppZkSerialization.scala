package mesosphere.marathon.state

import akka.http.scaladsl.marshalling.Marshaller
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.util.ByteString
import mesosphere.marathon.Protos.ServiceDefinition
import mesosphere.marathon.core.storage.IdResolver
import mesosphere.marathon.core.storage.impl.zk.{ZkId, ZkSerialized}

case class AppPathId(pathId: PathId) extends AnyVal
object AppPathId {
  val root = AppPathId(PathId.empty)
}

trait AppZkSerialization {
  implicit val appZkPathIdResolver = new IdResolver[AppPathId, ZkId, AppDefinition, ZkSerialized] {
    override def toStorageId(id: AppPathId): ZkId = {
      val path = id.pathId.path.mkString("_")
      val folder = path.hashCode % 16
      ZkId(s"/$folder/$path")
    }

    override def fromStorageId(key: ZkId): AppPathId = {
      AppPathId(PathId.fromSafePath(key.id.split("/").last))
    }
  }

  implicit val appZkMarshaller: Marshaller[AppDefinition, ZkSerialized] = Marshaller.opaque { (app: AppDefinition) =>
    ZkSerialized(ByteString(app.toProto.toByteArray))
  }

  implicit val appZkUnmarshaller: Unmarshaller[ZkSerialized, AppDefinition] =
    Unmarshaller.strict { (zk: ZkSerialized) =>
      val proto = ServiceDefinition.parseFrom(zk.bytes.toArray[Byte])
      AppDefinition.fromProto(proto)
  }
}

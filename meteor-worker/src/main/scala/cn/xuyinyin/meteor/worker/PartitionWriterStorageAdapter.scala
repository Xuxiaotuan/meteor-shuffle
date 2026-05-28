package cn.xuyinyin.meteor.worker

import cn.xuyinyin.meteor.transport.StorageAdapter
import cn.xuyinyin.meteor.common.Protocol.ShuffleId
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

/**
 * PartitionWriter 实现的 StorageAdapter。
 *
 * 供 Netty TransportServer 直接操作本地 PartitionWriter，
 * 绕过 Pekko Actor 消息序列化。
 */
class PartitionWriterStorageAdapter(
  storageDirs: Seq[String],
  writers: ConcurrentHashMap[String, PartitionWriter]
) extends StorageAdapter {

  override def selectDir(shuffleId: String, partitionIndex: Int): Path = {
    val idx = math.abs((shuffleId.hashCode + partitionIndex) % storageDirs.size)
    java.nio.file.Paths.get(storageDirs(idx))
  }

  override def write(key: String, data: Array[Byte]): Unit = {
    val parts = key.replaceFirst("^repl:", "").split(":")
    if (parts.length != 4) {
      throw new IllegalArgumentException(s"Malformed partition key: $key")
    }
    val appId = parts(0)
    val shuffleNum = parts(1).toInt
    val partIdx = parts(2).toInt
    val attempt = parts(3).toInt

    val writer = writers.computeIfAbsent(key, _ => {
      val dir = selectDir(key, partIdx)
      new PartitionWriter(dir, ShuffleId(appId, shuffleNum), partIdx, attempt)
    })
    writer.write(data)
  }

  override def readComplete(key: String): Option[Array[Byte]] = {
    val writer = writers.get(key)
    if (writer == null || !writer.isComplete) None
    else try { Some(writer.readAll()) } catch { case _: Exception => None }
  }

  override def markComplete(key: String): Unit = {
    val writer = writers.get(key)
    if (writer != null) writer.close()
  }
}

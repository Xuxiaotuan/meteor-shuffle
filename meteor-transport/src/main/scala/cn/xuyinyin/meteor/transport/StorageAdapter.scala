package cn.xuyinyin.meteor.transport

/**
 * 存储适配器 — TransportServer 通过此接口操作存储，不依赖 Worker 内部实现。
 * meteor-worker 提供 PartitionWriterStorageAdapter 实现。
 */
trait StorageAdapter {
  /** 写入数据到指定 key */
  def write(key: String, data: Array[Byte]): Unit

  /** 读取已完成的 partition 数据 */
  def readComplete(key: String): Option[Array[Byte]]

  /** 标记 partition 已完成（可被 fetch） */
  def markComplete(key: String): Unit

  /** 选择存储目录 */
  def selectDir(shuffleId: String, partitionIndex: Int): java.nio.file.Path
}

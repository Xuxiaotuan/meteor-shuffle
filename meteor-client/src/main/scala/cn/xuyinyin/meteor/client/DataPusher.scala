package cn.xuyinyin.meteor.client

import scala.collection.mutable

import cn.xuyinyin.meteor.common.Protocol._

/**
 * DataPusher — 批量缓冲 + 定时刷新
 *
 * 对标 Celeborn 的 DataPusher：
 *   - 按 partition 缓冲记录
 *   - 达到 batchSize 或 batchBytes 阈值时自动 flush
 *   - 空闲超时自动 flush（防止最后几条数据永不发送）
 *
 * 使用方式：
 *   DataPusher.push(shuffleId, partitionIndex, attempt, data, compression)
 *   DataPusher.flushPartition(shuffleId, partitionIndex) // mapper 结束时调用
 *
 * @param pushFn        实际推送回调
 * @param batchSize     批量大小（记录数），默认 64
 * @param batchBytes    批量大小（字节），默认 256KB
 * @param flushInterval 空闲刷新间隔
 */
class DataPusher(
  pushFn: (ShuffleId, Int, Int, Array[Byte], CompressionType) => Unit,
  batchSize: Int = 64,
  batchBytes: Int = 256 * 1024,
  flushIntervalMs: Long = 100
) {
  import DataPusher._

  // partitionKey(shuffleId:partitionIndex:attempt) → Buffer
  private val buffers = mutable.Map.empty[String, Buffer]
  private var lastFlushMs = System.currentTimeMillis()

  /** 推送一条记录（异步合并） */
  def push(
    shuffleId: ShuffleId,
    partitionIndex: Int,
    attemptNumber: Int,
    data: Array[Byte],
    compression: CompressionType = CompressNone
  ): Unit = {
    val key = s"${shuffleId.appId}:${shuffleId.shuffleNum}:$partitionIndex:$attemptNumber"
    val buf = buffers.getOrElseUpdate(key, Buffer.newBuilder)

    buf.add(data)
    val shouldFlush = buf.count >= batchSize || buf.bytes >= batchBytes

    if (shouldFlush) {
      doFlush(key, buf, shuffleId, partitionIndex, attemptNumber, compression)
    }
  }

  /** 强制刷新一个分区（mapper 结束时调用） */
  def flushPartition(
    shuffleId: ShuffleId,
    partitionIndex: Int,
    attemptNumber: Int,
    compression: CompressionType = CompressNone
  ): Unit = {
    val key = s"${shuffleId.appId}:${shuffleId.shuffleNum}:$partitionIndex:$attemptNumber"
    buffers.get(key).foreach { buf =>
      if (buf.count > 0) {
        doFlush(key, buf, shuffleId, partitionIndex, attemptNumber, compression)
      }
    }
  }

  /** 检查并刷新所有空闲超时的 buffer */
  def checkIdleFlush(): Unit = {
    val now = System.currentTimeMillis()
    if (now - lastFlushMs >= flushIntervalMs) {
      lastFlushMs = now
      // 收集需要刷新的 buffer（避免遍历时修改）
      val toFlush = buffers.collect {
        case (key, buf) if buf.count > 0 && (now - buf.lastAddMs >= flushIntervalMs) =>
          (key, buf)
      }
      toFlush.foreach { case (key, buf) =>
        // 从 key 反推 shuffleId / partitionIndex / attemptNumber
        val parts = key.split(":")
        if (parts.length == 4) {
          val sid = ShuffleId(parts(0), parts(1).toInt)
          doFlush(key, buf, sid, parts(2).toInt, parts(3).toInt, CompressNone)
        }
      }
    }
  }

  private def doFlush(
    key: String,
    buf: Buffer,
    shuffleId: ShuffleId,
    partitionIndex: Int,
    attemptNumber: Int,
    compression: CompressionType
  ): Unit = {
    if (buf.count == 0) return

    // 合并所有记录：4B length prefix + data
    val totalSize = buf.batches.iterator.map(_.length + 4).sum
    val merged = new Array[Byte](totalSize)
    var offset = 0
    buf.batches.foreach { chunk =>
      val len = chunk.length
      // Big-endian length prefix
      merged(offset) = ((len >> 24) & 0xFF).toByte
      merged(offset + 1) = ((len >> 16) & 0xFF).toByte
      merged(offset + 2) = ((len >> 8) & 0xFF).toByte
      merged(offset + 3) = (len & 0xFF).toByte
      System.arraycopy(chunk, 0, merged, offset + 4, len)
      offset += 4 + len
    }

    pushFn(shuffleId, partitionIndex, attemptNumber, merged, compression)
    buffers.remove(key)
    lastFlushMs = System.currentTimeMillis()
  }

  /** 缓冲区中的记录数 */
  def pendingRecords: Int = buffers.values.map(_.count).sum

  /** 缓冲区中的总字节数 */
  def pendingBytes: Long = buffers.values.map(_.bytes).sum

  /** 刷新所有缓冲区 */
  def flushAll(): Unit = {
    buffers.foreach { case (key, buf) =>
      if (buf.count > 0) {
        val parts = key.split(":")
        if (parts.length == 4) {
          val sid = ShuffleId(parts(0), parts(1).toInt)
          doFlush(key, buf, sid, parts(2).toInt, parts(3).toInt, CompressNone)
        }
      }
    }
  }
}

object DataPusher {
  private case class Buffer(
    batches: mutable.ArrayBuffer[Array[Byte]],
    var count: Int,
    var bytes: Long,
    var lastAddMs: Long
  ) {
    def add(data: Array[Byte]): Unit = {
      batches += data
      count += 1
      bytes += data.length
      lastAddMs = System.currentTimeMillis()
    }
  }
  private object Buffer {
    def newBuilder: Buffer = Buffer(
      mutable.ArrayBuffer.empty[Array[Byte]], 0, 0L, System.currentTimeMillis()
    )
  }
}

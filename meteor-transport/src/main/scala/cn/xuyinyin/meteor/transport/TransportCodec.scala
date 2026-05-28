package cn.xuyinyin.meteor.transport

import io.netty.buffer.{ByteBuf, Unpooled}
import java.nio.charset.StandardCharsets

/**
 * Netty 直连传输的帧格式定义 + 编解码器。
 *
 * Wire format:
 * ┌──────────────┬──────┬────────┬──────────────────────┐
 * │ frameLength  │ type │ reqId  │ body                 │
 * │  (4 bytes)   │(1 B) │ (4 B)  │ (variable)           │
 * └──────────────┴──────┴────────┴──────────────────────┘
 *
 * frameLength 包含自身的 4 字节（即 frameLength = 4 + 1 + 4 + body.length）。
 */
object TransportCodec {

  // -------- Message Types --------
  val TypePushRequest:  Byte = 0x01
  val TypePushResponse: Byte = 0x02
  val TypeFetchRequest: Byte = 0x03
  val TypeFetchResponse:Byte = 0x04
  val TypeReplicateReq: Byte = 0x05
  val TypeReplicateRsp: Byte = 0x06

  // -------- Frame ---------------

  def encodeFrame(msgType: Byte, reqId: Int, body: Array[Byte]): ByteBuf = {
    // frameLength = 4 (length field) + 1 (type) + 4 (reqId) + body.length
    val frameLen = 4 + 1 + 4 + body.length
    val buf = Unpooled.buffer(frameLen)
    buf.writeInt(frameLen)
    buf.writeByte(msgType.toInt)
    buf.writeInt(reqId)
    buf.writeBytes(body)
    buf
  }

  /** 解码帧头，返回 (type, reqId, body)。body 为新的 ByteBuf，调用方负责 release。 */
  def decodeFrame(buf: ByteBuf): Option[(Byte, Int, ByteBuf)] = {
    if (buf.readableBytes() < 9) return None  // min: 4 + 1 + 4
    val frameLen = buf.getInt(buf.readerIndex())
    if (buf.readableBytes() < frameLen) return None
    buf.readInt() // consume length
    val msgType = buf.readByte()
    val reqId   = buf.readInt()
    val bodyLen = frameLen - 9
    val body    = buf.readBytes(bodyLen)
    Some((msgType, reqId, body))
  }

  // ================================
  // PushData
  // ================================

  case class PushBody(
    appId: String,
    shuffleIndex: Int,
    partitionIndex: Int,
    attemptNumber: Int,
    data: Array[Byte],
    checksum: Long,
    compression: Byte    // 0 = none, 1 = LZ4, 2 = Snappy, 3 = Zstd
  )

  def encodePushBody(body: PushBody): Array[Byte] = {
    val appBytes = body.appId.getBytes(StandardCharsets.UTF_8)
    val buf = Unpooled.buffer()
    buf.writeShort(appBytes.length)
    buf.writeBytes(appBytes)
    buf.writeInt(body.shuffleIndex)
    buf.writeInt(body.partitionIndex)
    buf.writeInt(body.attemptNumber)
    buf.writeInt(body.data.length)
    buf.writeBytes(body.data)
    buf.writeLong(body.checksum)
    buf.writeByte(body.compression.toInt)
    val result = new Array[Byte](buf.readableBytes())
    buf.readBytes(result)
    buf.release()
    result
  }

  def decodePushBody(buf: ByteBuf): PushBody = {
    val appLen = buf.readShort()
    val appBytes = new Array[Byte](appLen)
    buf.readBytes(appBytes)
    val appId = new String(appBytes, StandardCharsets.UTF_8)
    val shuffleIndex = buf.readInt()
    val partitionIndex = buf.readInt()
    val attemptNumber = buf.readInt()
    val dataLen = buf.readInt()
    val data = new Array[Byte](dataLen)
    buf.readBytes(data)
    val checksum = buf.readLong()
    val compression = buf.readByte()
    PushBody(appId, shuffleIndex, partitionIndex, attemptNumber, data, checksum, compression)
  }

  // ================================
  // PushResponse
  // ================================

  def encodePushResponse(success: Boolean, errorMsg: String = ""): Array[Byte] = {
    val errBytes = errorMsg.getBytes(StandardCharsets.UTF_8)
    val buf = Unpooled.buffer()
    buf.writeBoolean(success)
    buf.writeShort(errBytes.length)
    if (errBytes.nonEmpty) buf.writeBytes(errBytes)
    val result = new Array[Byte](buf.readableBytes())
    buf.readBytes(result)
    buf.release()
    result
  }

  def decodePushResponse(buf: ByteBuf): (Boolean, String) = {
    val success  = buf.readBoolean()
    val errLen   = buf.readShort()
    val errBytes = new Array[Byte](errLen)
    if (errLen > 0) buf.readBytes(errBytes)
    val errMsg   = new String(errBytes, StandardCharsets.UTF_8)
    (success, errMsg)
  }

  // ================================
  // FetchData
  // ================================

  case class FetchBody(
    appId: String,
    shuffleIndex: Int,
    partitionIndex: Int,
    attemptNumber: Int
  )

  def encodeFetchBody(body: FetchBody): Array[Byte] = {
    val appBytes = body.appId.getBytes(StandardCharsets.UTF_8)
    val buf = Unpooled.buffer()
    buf.writeShort(appBytes.length)
    buf.writeBytes(appBytes)
    buf.writeInt(body.shuffleIndex)
    buf.writeInt(body.partitionIndex)
    buf.writeInt(body.attemptNumber)
    val result = new Array[Byte](buf.readableBytes())
    buf.readBytes(result)
    buf.release()
    result
  }

  def decodeFetchBody(buf: ByteBuf): FetchBody = {
    val appLen = buf.readShort()
    val appBytes = new Array[Byte](appLen)
    buf.readBytes(appBytes)
    val appId = new String(appBytes, StandardCharsets.UTF_8)
    val shuffleIndex = buf.readInt()
    val partitionIndex = buf.readInt()
    val attemptNumber = buf.readInt()
    FetchBody(appId, shuffleIndex, partitionIndex, attemptNumber)
  }

  // ================================
  // FetchResponse
  // ================================

  def encodeFetchResponse(success: Boolean, data: Array[Byte], errorMsg: String = ""): Array[Byte] = {
    val errBytes = errorMsg.getBytes(StandardCharsets.UTF_8)
    val buf = Unpooled.buffer()
    buf.writeBoolean(success)
    buf.writeInt(data.length)
    if (data.nonEmpty) buf.writeBytes(data)
    buf.writeShort(errBytes.length)
    if (errBytes.nonEmpty) buf.writeBytes(errBytes)
    val result = new Array[Byte](buf.readableBytes())
    buf.readBytes(result)
    buf.release()
    result
  }

  def decodeFetchResponse(buf: ByteBuf): (Boolean, Array[Byte], String) = {
    val success = buf.readBoolean()
    val dataLen = buf.readInt()
    val data    = if (dataLen > 0) { val arr = new Array[Byte](dataLen); buf.readBytes(arr); arr }
                  else Array.emptyByteArray
    val errLen  = buf.readShort()
    val errBytes = if (errLen > 0) { val arr = new Array[Byte](errLen); buf.readBytes(arr); arr }
                   else Array.emptyByteArray
    val errMsg  = new String(errBytes, StandardCharsets.UTF_8)
    (success, data, errMsg)
  }

  // ================================
  // ReplicateData
  // ================================
  // 复用 PushBody 的编码格式（除 type 不同外，字段完全一致）

  def encodeReplicateBody(body: PushBody): Array[Byte] = encodePushBody(body)
  def decodeReplicateBody(buf: ByteBuf): PushBody       = decodePushBody(buf)

  def encodeReplicateResponse(success: Boolean, errorMsg: String = ""): Array[Byte] =
    encodePushResponse(success, errorMsg)
  def decodeReplicateResponse(buf: ByteBuf): (Boolean, String) =
    decodePushResponse(buf)
}

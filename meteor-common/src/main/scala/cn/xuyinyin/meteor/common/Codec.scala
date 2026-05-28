package cn.xuyinyin.meteor.common

import java.util.zip.CRC32

/**
 * 数据压缩 + 校验 Codec
 *
 * 对标 Celeborn 的 CompressionCodec（LZ4 / Zstd / Snappy）
 * 内嵌 CRC32 校验，写入时附带，读取时验证。
 *
 * 帧格式：[4B magic][1B algo][4B originalLen][4B checksum][...compressed data]
 *   - magic:  0x4D45544F ("METEOR")
 *   - algo:   0=LZ4, 1=Snappy, 2=Zstd
 *   - originalLen: 原始数据长度（解压用）
 *   - checksum: CRC32 of 原始数据
 */
object Codec {

  val MAGIC: Int = 0x4D45544F  // "METEOR"
  val HEADER_SIZE: Int = 13    // 4 + 1 + 4 + 4

  sealed trait Algorithm { def id: Byte }
  case object LZ4    extends Algorithm { val id: Byte = 0 }
  case object Snappy extends Algorithm { val id: Byte = 1 }
  case object Zstd   extends Algorithm { val id: Byte = 2 }

  object Algorithm {
    def fromByte(b: Byte): Algorithm = b match {
      case 0 => LZ4
      case 1 => Snappy
      case 2 => Zstd
      case other => throw new IllegalArgumentException(s"Unknown codec algorithm: $other")
    }
  }

  // ================================
  // 压缩
  // ================================

  /** 压缩数据，返回带帧头的字节数组 */
  def compress(data: Array[Byte], algo: Algorithm): Array[Byte] = {
    val compressed = algo match {
      case LZ4    => compressLZ4(data)
      case Snappy => compressSnappy(data)
      case Zstd   => compressZstd(data)
    }

    val checksum = crc32(data)
    val buf = java.nio.ByteBuffer.allocate(HEADER_SIZE + compressed.length)
    buf.putInt(MAGIC)
    buf.put(algo.id)
    buf.putInt(data.length)        // originalLen
    buf.putInt(checksum)           // CRC32 of original
    buf.put(compressed)
    buf.array()
  }

  /** 解压数据，自动校验 CRC32 */
  def decompress(frame: Array[Byte]): Array[Byte] = {
    if (frame.length < HEADER_SIZE) {
      throw new IllegalArgumentException(s"Frame too short: ${frame.length} < $HEADER_SIZE")
    }

    val buf = java.nio.ByteBuffer.wrap(frame)
    val magic = buf.getInt()
    if (magic != MAGIC) {
      throw new IllegalArgumentException(f"Bad magic: 0x$magic%08X, expected 0x${MAGIC}%08X")
    }

    val algo = Algorithm.fromByte(buf.get())
    val originalLen = buf.getInt()
    val expectedCrc = buf.getInt()

    val compressed = new Array[Byte](frame.length - HEADER_SIZE)
    buf.get(compressed)

    val decompressed = algo match {
      case LZ4    => decompressLZ4(compressed, originalLen)
      case Snappy => decompressSnappy(compressed)
      case Zstd   => decompressZstd(compressed)
    }

    // CRC32 校验
    val actualCrc = crc32(decompressed)
    if (actualCrc != expectedCrc) {
      throw new IllegalStateException(
        f"CRC32 mismatch: actual=0x$actualCrc%08X, expected=0x$expectedCrc%08X"
      )
    }

    decompressed
  }

  /** 仅计算 CRC32（不压缩，用于快速校验原始数据） */
  def crc32(data: Array[Byte]): Int = {
    val crc = new CRC32()
    crc.update(data)
    crc.getValue.toInt
  }

  /**
   * 按压缩字节解压（不带 Meteor 帧头的原始压缩数据）。
   * compression: 0=none, 1=LZ4, 2=Snappy, 3=Zstd
   * 用于 Transport 层直接解压。
   */
  def decompressByByte(data: Array[Byte], compression: Byte): Array[Byte] = compression match {
    case 0 => data // 未压缩
    case 1 =>
      // LZ4 需要原始长度，从压缩数据估算（LZ4 最大压缩比 ~1:255，用 256x 估算足够）
      val estimateLen = data.length * 256
      val decompressor = net.jpountz.lz4.LZ4Factory.fastestInstance().fastDecompressor()
      decompressor.decompress(data, estimateLen)
    case 2 => org.xerial.snappy.Snappy.uncompress(data)
    case 3 =>
      val outSize = com.github.luben.zstd.Zstd.decompressedSize(data)
      com.github.luben.zstd.Zstd.decompress(data, outSize.toInt)
    case _ => throw new IllegalArgumentException(s"Unknown compression byte: $compression")
  }

  /** 尝试解压帧头，返回是否是合法的 Meteor 帧 */
  def isMeteorFrame(data: Array[Byte]): Boolean = {
    if (data.length < 4) return false
    val magic = java.nio.ByteBuffer.wrap(data).getInt()
    magic == MAGIC
  }

  // ================================
  // LZ4
  // ================================

  private def compressLZ4(data: Array[Byte]): Array[Byte] = {
    val compressor = net.jpountz.lz4.LZ4Factory.fastestInstance().fastCompressor()
    compressor.compress(data)
  }

  private def decompressLZ4(compressed: Array[Byte], originalLen: Int): Array[Byte] = {
    val decompressor = net.jpountz.lz4.LZ4Factory.fastestInstance().fastDecompressor()
    decompressor.decompress(compressed, originalLen)
  }

  // ================================
  // Snappy
  // ================================

  private def compressSnappy(data: Array[Byte]): Array[Byte] = {
    org.xerial.snappy.Snappy.compress(data)
  }

  private def decompressSnappy(compressed: Array[Byte]): Array[Byte] = {
    org.xerial.snappy.Snappy.uncompress(compressed)
  }

  // ================================
  // Zstd
  // ================================

  private def compressZstd(data: Array[Byte]): Array[Byte] = {
    com.github.luben.zstd.Zstd.compress(data)
  }

  private def decompressZstd(compressed: Array[Byte]): Array[Byte] = {
    com.github.luben.zstd.Zstd.decompress(compressed, com.github.luben.zstd.Zstd.decompressedSize(compressed).toInt)
  }
}

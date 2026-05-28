package cn.xuyinyin.meteor.common

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CodecSpec extends AnyWordSpec with Matchers {

  "Codec" should {

    "compress and decompress with LZ4" in {
      val original = "Hello, Meteor Shuffle! This is a test payload.".getBytes("UTF-8")
      val compressed = Codec.compress(original, Codec.LZ4)
      val decompressed = Codec.decompress(compressed)

      decompressed shouldEqual original
      Codec.isMeteorFrame(compressed) shouldBe true
    }

    "compress and decompress with Snappy" in {
      val original = "Snappy compression test data with some重复内容重复内容".getBytes("UTF-8")
      val compressed = Codec.compress(original, Codec.Snappy)
      val decompressed = Codec.decompress(compressed)

      decompressed shouldEqual original
    }

    "compress and decompress with Zstd" in {
      // Use compressible data (random data may not compress)
      val original = ("Zstd test data " * 500).getBytes("UTF-8")
      val compressed = Codec.compress(original, Codec.Zstd)
      val decompressed = Codec.decompress(compressed)

      decompressed shouldEqual original
      compressed.length should be < original.length  // Zstd should compress
    }

    "detect CRC32 corruption" in {
      val original = "Important data".getBytes("UTF-8")
      val compressed = Codec.compress(original, Codec.LZ4)

      // Corrupt the data
      compressed(compressed.length - 1) = (compressed(compressed.length - 1) ^ 0xFF).toByte

      an[IllegalStateException] should be thrownBy {
        Codec.decompress(compressed)
      }
    }

    "detect bad magic" in {
      val badFrame = new Array[Byte](20)
      // Not a Meteor frame
      an[IllegalArgumentException] should be thrownBy {
        Codec.decompress(badFrame)
      }
    }

    "detect unknown algorithm" in {
      val frame = new Array[Byte](20)
      frame(0) = 0x4D.toByte  // M
      frame(1) = 0x45.toByte  // E
      frame(2) = 0x54.toByte  // T
      frame(3) = 0x4F.toByte  // O
      frame(4) = 99.toByte    // Unknown algorithm

      an[IllegalArgumentException] should be thrownBy {
        Codec.decompress(frame)
      }
    }

    "identify Meteor frames" in {
      val original = "test".getBytes("UTF-8")
      val compressed = Codec.compress(original, Codec.LZ4)

      Codec.isMeteorFrame(compressed) shouldBe true
      Codec.isMeteorFrame(Array[Byte](1, 2, 3, 4)) shouldBe false
      Codec.isMeteorFrame(Array[Byte](0)) shouldBe false
    }

    "compute CRC32 consistently" in {
      val data = "consistent".getBytes("UTF-8")
      Codec.crc32(data) shouldEqual Codec.crc32(data)
    }

    "handle empty data" in {
      val original = Array.empty[Byte]
      val compressed = Codec.compress(original, Codec.LZ4)
      val decompressed = Codec.decompress(compressed)

      decompressed shouldEqual original
    }

    "achieve compression on large data" in {
      // Highly compressible data
      val original = ("A" * 10000).getBytes("UTF-8")
      val lz4 = Codec.compress(original, Codec.LZ4)
      val snappy = Codec.compress(original, Codec.Snappy)
      val zstd = Codec.compress(original, Codec.Zstd)

      lz4.length should be < original.length
      snappy.length should be < original.length
      zstd.length should be < original.length

      // All should decompress correctly
      Codec.decompress(lz4) shouldEqual original
      Codec.decompress(snappy) shouldEqual original
      Codec.decompress(zstd) shouldEqual original
    }
  }
}

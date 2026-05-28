package cn.xuyinyin.meteor.worker

import java.nio.file.Files
import java.util.concurrent.ConcurrentHashMap
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PartitionWriterStorageAdapterSpec extends AnyWordSpec with Matchers {

  "PartitionWriterStorageAdapter" should {

    "write and read a completed primary partition using shared writers" in {
      val dir = Files.createTempDirectory("meteor-storage-primary")
      val writers = new ConcurrentHashMap[String, PartitionWriter]()
      val adapter = new PartitionWriterStorageAdapter(Seq(dir.toString), writers)
      val key = "app:1:2:3"
      val payload = "primary".getBytes("UTF-8")

      adapter.write(key, payload)
      writers.containsKey(key) shouldBe true
      adapter.readComplete(key) shouldBe None
      adapter.markComplete(key)
      adapter.readComplete(key).map(_.toSeq) shouldEqual Some(payload.toSeq)
    }

    "write and read a completed replica partition" in {
      val dir = Files.createTempDirectory("meteor-storage-replica")
      val writers = new ConcurrentHashMap[String, PartitionWriter]()
      val adapter = new PartitionWriterStorageAdapter(Seq(dir.toString), writers)
      val key = "repl:app:1:2:3"
      val payload = "replica".getBytes("UTF-8")

      adapter.write(key, payload)
      adapter.markComplete(key)
      adapter.readComplete(key).map(_.toSeq) shouldEqual Some(payload.toSeq)
    }

    "reject malformed keys" in {
      val dir = Files.createTempDirectory("meteor-storage-bad-key")
      val writers = new ConcurrentHashMap[String, PartitionWriter]()
      val adapter = new PartitionWriterStorageAdapter(Seq(dir.toString), writers)

      an[IllegalArgumentException] should be thrownBy {
        adapter.write("bad-key", Array[Byte](1))
      }
    }
  }
}

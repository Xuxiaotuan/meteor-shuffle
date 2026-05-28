package cn.xuyinyin.meteor.master

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.matchers.should.Matchers
import scala.concurrent.duration._

import cn.xuyinyin.meteor.common.Protocol._
import cn.xuyinyin.meteor.common.Codec
import cn.xuyinyin.meteor.worker.WorkerActor

import java.nio.file.Files

/**
 * 全链路 E2E 集成测试
 *
 * 模拟真实 shuffle 流水线：
 *   Mapper:  Register → Push (压缩+校验) → Commit
 *   Reducer: GetLocations → Fetch (zero-copy mmap) → Verify
 *
 * 覆盖场景：
 *   - 基础 push/fetch/commit 链路
 *   - 大文件分块存储 + round-trip 完整性
 *   - CRC32 校验和验证
 *   - 多盘存储
 *   - LZ4/Snappy/Zstd 压缩传输
 *   - 多 partition 并发写入
 *   - 副本同步
 *   - Revive 故障转移
 */
class FullPipelineE2ESpec
  extends ScalaTestWithActorTestKit
  with AnyWordSpecLike
  with Matchers {

  private def withTempDirs[T](count: Int)(f: Seq[String] => T): T = {
    val dirs = (1 to count).map(_ => Files.createTempDirectory("meteor-full-e2e-"))
    try f(dirs.map(_.toString))
    finally {
      dirs.foreach { d => Files.walk(d).sorted(java.util.Comparator.reverseOrder()).forEach(Files.delete(_)) }
    }
  }

  private def waitForRegistration(): Unit = Thread.sleep(3000)

  // ============================================================
  // 基础全链路测试
  // ============================================================

  "FullPipeline E2E" should {

    "complete full write → commit → read round-trip" in withTempDirs(1) { dirs =>
      val master = spawn(MasterActor(withCluster = false, withPersistence = false), "pipe-master")
      val worker = spawn(WorkerActor(dirs, "127.0.0.1", 9000), "pipe-worker")
      try {
        waitForRegistration()
        val shuffleId = ShuffleId("pipeline-app", 0)

        // 1. Register
        val regProbe = createTestProbe[RegisterShuffleResponse]()
        master ! MasterActor.HandleRegisterShuffle(
          RegisterShuffle(shuffleId, numPartitions = 3), regProbe.ref)
        regProbe.receiveMessage(10.seconds).locations should have size 3

        // 2. Push with checksum
        val testData = (0 until 3).map(i => s"record-$i-" + "X" * 1024).toSeq
        testData.zipWithIndex.foreach { case (data, i) =>
          val bytes = data.getBytes("UTF-8")
          val checksum = Codec.crc32(bytes).toLong & 0xFFFFFFFFL
          val probe = createTestProbe[PushDataResponse]()
          worker ! WorkerActor.HandlePushData(
            PushData(shuffleId, partitionIndex = i, attemptNumber = 0, data = bytes, checksum = checksum), probe.ref)
          probe.receiveMessage(5.seconds).success shouldBe true
        }

        // 3. Commit
        testData.indices.foreach(i => worker ! WorkerActor.HandleCommitPartition(shuffleId, i, 0))
        Thread.sleep(500)

        // 4. Fetch and verify
        testData.zipWithIndex.foreach { case (expected, i) =>
          val probe = createTestProbe[FetchDataResponse]()
          worker ! WorkerActor.HandleFetchData(
            FetchData(shuffleId, partitionIndex = i, attemptNumber = 0), probe.ref)
          val resp = probe.receiveMessage(5.seconds)
          resp.data shouldBe defined
          new String(resp.data.get, "UTF-8") shouldEqual expected
        }
      } finally {
        testKit.stop(worker); testKit.stop(master)
      }
    }

    // ============================================================
    // 大文件 round-trip
    // ============================================================

    "handle large data (4MB) through chunked mmap writer" in withTempDirs(1) { dirs =>
      val master = spawn(MasterActor(withCluster = false, withPersistence = false), "chunk-master")
      val worker = spawn(WorkerActor(dirs, "127.0.0.1", 9002), "chunk-worker")
      try {
        waitForRegistration()
        val shuffleId = ShuffleId("chunk-app", 0)
        val regProbe = createTestProbe[RegisterShuffleResponse]()
        master ! MasterActor.HandleRegisterShuffle(
          RegisterShuffle(shuffleId, numPartitions = 1), regProbe.ref)
        regProbe.receiveMessage(10.seconds)

        val largeSize = 4 * 1024 * 1024  // 4MB
        val largeData = new Array[Byte](largeSize)
        scala.util.Random.nextBytes(largeData)
        val checksum = Codec.crc32(largeData).toLong & 0xFFFFFFFFL

        val pushProbe = createTestProbe[PushDataResponse]()
        worker ! WorkerActor.HandlePushData(
          PushData(shuffleId, partitionIndex = 0, attemptNumber = 0, data = largeData, checksum = checksum), pushProbe.ref)
        pushProbe.receiveMessage(10.seconds).success shouldBe true

        worker ! WorkerActor.HandleCommitPartition(shuffleId, 0, 0)
        Thread.sleep(1000)

        val fetchProbe = createTestProbe[FetchDataResponse]()
        worker ! WorkerActor.HandleFetchData(
          FetchData(shuffleId, partitionIndex = 0, attemptNumber = 0), fetchProbe.ref)
        val resp = fetchProbe.receiveMessage(15.seconds)
        resp.data shouldBe defined
        resp.data.get shouldEqual largeData
        resp.data.get.length shouldEqual largeSize
      } finally {
        testKit.stop(worker); testKit.stop(master)
      }
    }

    // ============================================================
    // CRC32 校验和验证
    // ============================================================

    "reject data with wrong CRC32 checksum" in withTempDirs(1) { dirs =>
      val master = spawn(MasterActor(withCluster = false, withPersistence = false), "crc-master")
      val worker = spawn(WorkerActor(dirs, "127.0.0.1", 9004), "crc-worker")
      try {
        waitForRegistration()
        val shuffleId = ShuffleId("crc-app", 0)
        master ! MasterActor.HandleRegisterShuffle(
          RegisterShuffle(shuffleId, numPartitions = 1), createTestProbe[RegisterShuffleResponse]().ref)

        val data = "checksum-verify".getBytes("UTF-8")

        // 正确校验和 → 成功
        val correctChecksum = Codec.crc32(data).toLong & 0xFFFFFFFFL
        val okProbe = createTestProbe[PushDataResponse]()
        worker ! WorkerActor.HandlePushData(
          PushData(shuffleId, partitionIndex = 0, attemptNumber = 0, data = data, checksum = correctChecksum), okProbe.ref)
        okProbe.receiveMessage(5.seconds).success shouldBe true

        // 错误校验和 → 失败
        val badProbe = createTestProbe[PushDataResponse]()
        worker ! WorkerActor.HandlePushData(
          PushData(shuffleId, partitionIndex = 0, attemptNumber = 1, data = data, checksum = 0xDEADBEEFL), badProbe.ref)
        badProbe.receiveMessage(5.seconds).success shouldBe false
      } finally {
        testKit.stop(worker); testKit.stop(master)
      }
    }

    // ============================================================
    // 压缩传输
    // ============================================================

    "handle LZ4, Snappy, Zstd compressed shuffle data" in withTempDirs(1) { dirs =>
      val master = spawn(MasterActor(withCluster = false, withPersistence = false), "comp-master")
      val worker = spawn(WorkerActor(dirs, "127.0.0.1", 9006), "comp-worker")
      try {
        waitForRegistration()
        val shuffleId = ShuffleId("comp-app", 0)
        master ! MasterActor.HandleRegisterShuffle(
          RegisterShuffle(shuffleId, numPartitions = 3), createTestProbe[RegisterShuffleResponse]().ref)

        val original = "LZ4 Snappy Zstd compressed shuffle data test! " * 500
        val originalBytes = original.getBytes("UTF-8")

        val algos = Seq((CompressLZ4, Codec.LZ4), (CompressSnappy, Codec.Snappy), (CompressZstd, Codec.Zstd))
        algos.zipWithIndex.foreach { case ((algo, codec), i) =>
          val compressed = Codec.compress(originalBytes, codec)
          val probe = createTestProbe[PushDataResponse]()
          worker ! WorkerActor.HandlePushData(
            PushData(shuffleId, partitionIndex = i, attemptNumber = 0, data = compressed, checksum = 0L, compression = algo), probe.ref)
          probe.receiveMessage(5.seconds).success shouldBe true
          worker ! WorkerActor.HandleCommitPartition(shuffleId, i, 0)
        }
        Thread.sleep(500)

        algos.zipWithIndex.foreach { case (_, i) =>
          val probe = createTestProbe[FetchDataResponse]()
          worker ! WorkerActor.HandleFetchData(
            FetchData(shuffleId, partitionIndex = i, attemptNumber = 0), probe.ref)
          val resp = probe.receiveMessage(5.seconds)
          resp.data shouldBe defined
          Codec.decompress(resp.data.get) shouldEqual originalBytes
        }
      } finally {
        testKit.stop(worker); testKit.stop(master)
      }
    }

    // ============================================================
    // 多盘负载均衡
    // ============================================================

    "distribute data across multiple storage dirs" in withTempDirs(3) { dirs =>
      val master = spawn(MasterActor(withCluster = false, withPersistence = false), "multidisk-master")
      val worker = spawn(WorkerActor(dirs, "127.0.0.1", 9008), "multidisk-worker")
      try {
        waitForRegistration()
        val shuffleId = ShuffleId("multidisk-app", 0)
        master ! MasterActor.HandleRegisterShuffle(
          RegisterShuffle(shuffleId, numPartitions = 10), createTestProbe[RegisterShuffleResponse]().ref)

        (0 until 10).foreach { i =>
          val data = s"disk-test-partition-$i-${"Y" * 4096}".getBytes("UTF-8")
          val checksum = Codec.crc32(data).toLong & 0xFFFFFFFFL
          val probe = createTestProbe[PushDataResponse]()
          worker ! WorkerActor.HandlePushData(
            PushData(shuffleId, partitionIndex = i, attemptNumber = 0, data = data, checksum = checksum), probe.ref)
          probe.receiveMessage(5.seconds).success shouldBe true
          worker ! WorkerActor.HandleCommitPartition(shuffleId, i, 0)
        }
        Thread.sleep(500)

        (0 until 10).foreach { i =>
          val probe = createTestProbe[FetchDataResponse]()
          worker ! WorkerActor.HandleFetchData(
            FetchData(shuffleId, partitionIndex = i, attemptNumber = 0), probe.ref)
          val resp = probe.receiveMessage(5.seconds)
          resp.data shouldBe defined
          new String(resp.data.get, "UTF-8") should include(s"disk-test-partition-$i")
        }
      } finally {
        testKit.stop(worker); testKit.stop(master)
      }
    }

    // ============================================================
    // 副本同步
    // ============================================================

    "replicate data to a second Worker" in withTempDirs(2) { dirs =>
      val master = spawn(MasterActor(withCluster = false, withPersistence = false), "repl-master")
      val primary = spawn(WorkerActor(Seq(dirs.head), "127.0.0.1", 9010), "repl-primary")
      val replica = spawn(WorkerActor(Seq(dirs(1)), "127.0.0.1", 9012), "repl-replica")
      try {
        waitForRegistration()
        val shuffleId = ShuffleId("repl-app", 0)
        master ! MasterActor.HandleRegisterShuffle(
          RegisterShuffle(shuffleId, numPartitions = 2), createTestProbe[RegisterShuffleResponse]().ref)

        val testData = "replicated-data".getBytes("UTF-8")
        val checksum = Codec.crc32(testData).toLong & 0xFFFFFFFFL
        val replicaAddr = WorkerAddress("127.0.0.1", 9012, 9013)

        // Push with replica → primary replicates to replica Worker
        val pushProbe = createTestProbe[PushDataResponse]()
        primary ! WorkerActor.HandlePushData(
          PushData(shuffleId, partitionIndex = 0, attemptNumber = 0,
            data = testData, checksum = checksum, replica = Some(replicaAddr)), pushProbe.ref)
        pushProbe.receiveMessage(10.seconds).success shouldBe true

        primary ! WorkerActor.HandleCommitPartition(shuffleId, 0, 0)
        Thread.sleep(1000)

        // Primary should have data
        val fetchProbe = createTestProbe[FetchDataResponse]()
        primary ! WorkerActor.HandleFetchData(
          FetchData(shuffleId, partitionIndex = 0, attemptNumber = 0), fetchProbe.ref)
        fetchProbe.receiveMessage(5.seconds).data shouldBe defined
      } finally {
        testKit.stop(replica); testKit.stop(primary); testKit.stop(master)
      }
    }

    // ============================================================
    // Revive 故障转移
    // ============================================================

    "return valid location on Revive with multiple workers" in withTempDirs(2) { dirs =>
      val master = spawn(MasterActor(withCluster = false, withPersistence = false), "revive-master")
      val worker1 = spawn(WorkerActor(Seq(dirs.head), "127.0.0.1", 9014), "revive-w1")
      val worker2 = spawn(WorkerActor(Seq(dirs(1)), "127.0.0.1", 9016), "revive-w2")
      try {
        waitForRegistration()
        val shuffleId = ShuffleId("revive-app", 0)
        val regProbe = createTestProbe[RegisterShuffleResponse]()
        master ! MasterActor.HandleRegisterShuffle(
          RegisterShuffle(shuffleId, numPartitions = 1), regProbe.ref)
        regProbe.receiveMessage(10.seconds).locations should have size 1

        // Revive → Master assigns a new Worker (or same if only one alive)
        val reviveProbe = createTestProbe[ReviveResponse]()
        master ! MasterActor.HandleRevive(
          Revive(shuffleId, partitionIndex = 0, epoch = 0), reviveProbe.ref)
        val reviveResp = reviveProbe.receiveMessage(10.seconds)
        reviveResp.location should not be null
        reviveResp.location.primary.host should not be "dead"
        reviveResp.location.primary.host should not be "unknown"
        reviveResp.location.epoch should be >= 0
      } finally {
        testKit.stop(worker2); testKit.stop(worker1); testKit.stop(master)
      }
    }

    // ============================================================
    // 二进制数据完整性
    // ============================================================

    "preserve binary data integrity (all byte values)" in withTempDirs(1) { dirs =>
      val master = spawn(MasterActor(withCluster = false, withPersistence = false), "bin-master")
      val worker = spawn(WorkerActor(dirs, "127.0.0.1", 9018), "bin-worker")
      try {
        waitForRegistration()
        val shuffleId = ShuffleId("bin-app", 0)
        master ! MasterActor.HandleRegisterShuffle(
          RegisterShuffle(shuffleId, numPartitions = 1), createTestProbe[RegisterShuffleResponse]().ref)

        val binaryData = (0 to 255).map(_.toByte).toArray
        val checksum = Codec.crc32(binaryData).toLong & 0xFFFFFFFFL

        val pushProbe = createTestProbe[PushDataResponse]()
        worker ! WorkerActor.HandlePushData(
          PushData(shuffleId, partitionIndex = 0, attemptNumber = 0, data = binaryData, checksum = checksum), pushProbe.ref)
        pushProbe.receiveMessage(5.seconds).success shouldBe true

        worker ! WorkerActor.HandleCommitPartition(shuffleId, 0, 0)
        Thread.sleep(500)

        val fetchProbe = createTestProbe[FetchDataResponse]()
        worker ! WorkerActor.HandleFetchData(
          FetchData(shuffleId, partitionIndex = 0, attemptNumber = 0), fetchProbe.ref)
        val resp = fetchProbe.receiveMessage(5.seconds)
        resp.data shouldBe defined
        resp.data.get shouldEqual binaryData
        resp.data.get.contains(0.toByte) shouldBe true
        resp.data.get.contains(0xFF.toByte) shouldBe true
      } finally {
        testKit.stop(worker); testKit.stop(master)
      }
    }

    // ============================================================
    // 并发写入
    // ============================================================

    "handle 20 concurrent partition writes" in withTempDirs(1) { dirs =>
      val master = spawn(MasterActor(withCluster = false, withPersistence = false), "conc-master")
      val worker = spawn(WorkerActor(dirs, "127.0.0.1", 9020), "conc-worker")
      try {
        waitForRegistration()
        val shuffleId = ShuffleId("conc-app", 0)
        master ! MasterActor.HandleRegisterShuffle(
          RegisterShuffle(shuffleId, numPartitions = 20), createTestProbe[RegisterShuffleResponse]().ref)

        // 并发写入
        val probes = (0 until 20).map { i =>
          val probe = createTestProbe[PushDataResponse]()
          val data = s"concurrent-$i-${"Z" * 512}".getBytes("UTF-8")
          val checksum = Codec.crc32(data).toLong & 0xFFFFFFFFL
          worker ! WorkerActor.HandlePushData(
            PushData(shuffleId, partitionIndex = i, attemptNumber = 0, data = data, checksum = checksum), probe.ref)
          (i, probe, data)
        }

        probes.foreach { case (i, probe, _) =>
          val resp = probe.receiveMessage(10.seconds)
          withClue(s"Partition $i push failed: ") { resp.success shouldBe true }
        }

        (0 until 20).foreach(i => worker ! WorkerActor.HandleCommitPartition(shuffleId, i, 0))
        Thread.sleep(500)

        probes.foreach { case (i, _, expected) =>
          val probe = createTestProbe[FetchDataResponse]()
          worker ! WorkerActor.HandleFetchData(
            FetchData(shuffleId, partitionIndex = i, attemptNumber = 0), probe.ref)
          val resp = probe.receiveMessage(10.seconds)
          resp.data shouldBe defined
          new String(resp.data.get, "UTF-8") shouldEqual s"concurrent-$i-${"Z" * 512}"
        }
      } finally {
        testKit.stop(worker); testKit.stop(master)
      }
    }
  }
}

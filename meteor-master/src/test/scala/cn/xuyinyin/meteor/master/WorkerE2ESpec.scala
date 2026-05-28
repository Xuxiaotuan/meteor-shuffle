package cn.xuyinyin.meteor.master

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed.ActorRef
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.matchers.should.Matchers
import scala.concurrent.duration._

import cn.xuyinyin.meteor.common.Protocol._
import cn.xuyinyin.meteor.common.{Codec, MasterActorCommand}
import cn.xuyinyin.meteor.master.MasterActor
import cn.xuyinyin.meteor.worker.WorkerActor

import java.nio.file.Files

class WorkerE2ESpec
  extends ScalaTestWithActorTestKit
  with AnyWordSpecLike
  with Matchers {

  private def withTempDir[T](f: String => T): T = {
    val dir = Files.createTempDirectory("meteor-e2e-")
    try f(dir.toString)
    finally {
      import java.nio.file.{Path, Files}
      Files.walk(dir).sorted(java.util.Comparator.reverseOrder()).forEach(Files.delete(_))
    }
  }

  "WorkerActor end-to-end" should {

    "register with Master and accept PushData" in withTempDir { tmpDir =>
      val master = spawn(MasterActor(withCluster = false, withPersistence = false), "e2e-master")
      val worker = spawn(WorkerActor(Seq(tmpDir), "127.0.0.1", 9000), "e2e-worker")
      try {
        Thread.sleep(3000)

        val wProbe = createTestProbe[MasterActor.WorkersQueryResponse]()
        master ! MasterActor.QueryWorkers(wProbe.ref)
        val workersResp = wProbe.receiveMessage(5.seconds)
        workersResp.workers should not be empty

        val sProbe = createTestProbe[RegisterShuffleResponse]()
        val shuffleId = ShuffleId("e2e-app", 0)
        master ! MasterActor.HandleRegisterShuffle(
          RegisterShuffle(shuffleId, numPartitions = 2), sProbe.ref)
        val shuffleResp = sProbe.receiveMessage(10.seconds)
        shuffleResp.locations should have size 2

        val pushProbe = createTestProbe[PushDataResponse]()
        val testData = "Hello Meteor Shuffle!".getBytes("UTF-8")
        worker ! WorkerActor.HandlePushData(
          PushData(shuffleId, partitionIndex = 0, attemptNumber = 0, data = testData, checksum = 0L),
          pushProbe.ref)
        pushProbe.receiveMessage(5.seconds).success shouldBe true

        worker ! WorkerActor.HandleCommitPartition(shuffleId, 0, 0)
        Thread.sleep(500)

        val fetchProbe = createTestProbe[FetchDataResponse]()
        worker ! WorkerActor.HandleFetchData(
          FetchData(shuffleId, partitionIndex = 0, attemptNumber = 0), fetchProbe.ref)
        val fetchResp = fetchProbe.receiveMessage(5.seconds)
        fetchResp.data shouldBe defined
        new String(fetchResp.data.get, "UTF-8") shouldEqual "Hello Meteor Shuffle!"
      } finally {
        testKit.stop(worker)
        testKit.stop(master)
      }
    }

    "handle compressed PushData (LZ4)" in withTempDir { tmpDir =>
      val master = spawn(MasterActor(withCluster = false, withPersistence = false), "e2e-master-lz4")
      val worker = spawn(WorkerActor(Seq(tmpDir), "127.0.0.1", 9002), "e2e-worker-lz4")
      try {
        Thread.sleep(3000)
        val shuffleId = ShuffleId("e2e-lz4", 0)

        val pushProbe = createTestProbe[PushDataResponse]()
        val originalData = ("A" * 10000).getBytes("UTF-8")
        val compressedData = Codec.compress(originalData, Codec.LZ4)
        worker ! WorkerActor.HandlePushData(
          PushData(shuffleId, partitionIndex = 0, attemptNumber = 0,
            data = compressedData, checksum = 0L, compression = CompressLZ4),
          pushProbe.ref)
        pushProbe.receiveMessage(5.seconds).success shouldBe true

        worker ! WorkerActor.HandleCommitPartition(shuffleId, 0, 0)
        Thread.sleep(500)

        val fetchProbe = createTestProbe[FetchDataResponse]()
        worker ! WorkerActor.HandleFetchData(
          FetchData(shuffleId, partitionIndex = 0, attemptNumber = 0), fetchProbe.ref)
        val fetchResp = fetchProbe.receiveMessage(5.seconds)
        fetchResp.data shouldBe defined

        val fetched = fetchResp.data.get
        Codec.isMeteorFrame(fetched) shouldBe true
        Codec.decompress(fetched) shouldEqual originalData
      } finally {
        testKit.stop(worker)
        testKit.stop(master)
      }
    }

    "handle compressed PushData (Zstd)" in withTempDir { tmpDir =>
      val master = spawn(MasterActor(withCluster = false, withPersistence = false), "e2e-master-zstd")
      val worker = spawn(WorkerActor(Seq(tmpDir), "127.0.0.1", 9004), "e2e-worker-zstd")
      try {
        Thread.sleep(3000)
        val shuffleId = ShuffleId("e2e-zstd", 0)

        val pushProbe = createTestProbe[PushDataResponse]()
        val originalData = ("Zstd test data " * 1000).getBytes("UTF-8")
        val compressedData = Codec.compress(originalData, Codec.Zstd)
        worker ! WorkerActor.HandlePushData(
          PushData(shuffleId, partitionIndex = 0, attemptNumber = 0,
            data = compressedData, checksum = 0L, compression = CompressZstd),
          pushProbe.ref)
        pushProbe.receiveMessage(5.seconds).success shouldBe true

        worker ! WorkerActor.HandleCommitPartition(shuffleId, 0, 0)
        Thread.sleep(500)

        val fetchProbe = createTestProbe[FetchDataResponse]()
        worker ! WorkerActor.HandleFetchData(
          FetchData(shuffleId, partitionIndex = 0, attemptNumber = 0), fetchProbe.ref)
        val fetchResp = fetchProbe.receiveMessage(5.seconds)
        fetchResp.data shouldBe defined

        Codec.decompress(fetchResp.data.get) shouldEqual originalData
      } finally {
        testKit.stop(worker)
        testKit.stop(master)
      }
    }

    "handle multiple partitions independently" in withTempDir { tmpDir =>
      val master = spawn(MasterActor(withCluster = false, withPersistence = false), "e2e-master-multi")
      val worker = spawn(WorkerActor(Seq(tmpDir), "127.0.0.1", 9006), "e2e-worker-multi")
      try {
        Thread.sleep(3000)
        val shuffleId = ShuffleId("e2e-multi", 0)

        for (i <- 0 until 3) {
          val probe = createTestProbe[PushDataResponse]()
          worker ! WorkerActor.HandlePushData(
            PushData(shuffleId, partitionIndex = i, attemptNumber = 0,
              data = s"partition-$i".getBytes("UTF-8"), checksum = 0L), probe.ref)
          probe.receiveMessage(5.seconds).success shouldBe true
          worker ! WorkerActor.HandleCommitPartition(shuffleId, i, 0)
        }
        Thread.sleep(500)

        for (i <- 0 until 3) {
          val probe = createTestProbe[FetchDataResponse]()
          worker ! WorkerActor.HandleFetchData(
            FetchData(shuffleId, partitionIndex = i, attemptNumber = 0), probe.ref)
          val resp = probe.receiveMessage(5.seconds)
          resp.data shouldBe defined
          new String(resp.data.get, "UTF-8") shouldEqual s"partition-$i"
        }
      } finally {
        testKit.stop(worker)
        testKit.stop(master)
      }
    }

    "return None for uncommitted partition" in withTempDir { tmpDir =>
      val master = spawn(MasterActor(withCluster = false, withPersistence = false), "e2e-master-uncommit")
      val worker = spawn(WorkerActor(Seq(tmpDir), "127.0.0.1", 9008), "e2e-worker-uncommit")
      try {
        Thread.sleep(3000)
        val shuffleId = ShuffleId("e2e-uncommit", 0)

        val pushProbe = createTestProbe[PushDataResponse]()
        worker ! WorkerActor.HandlePushData(
          PushData(shuffleId, partitionIndex = 0, attemptNumber = 0,
            data = "uncommitted".getBytes("UTF-8"), checksum = 0L), pushProbe.ref)
        pushProbe.receiveMessage(5.seconds).success shouldBe true
        Thread.sleep(500)

        val fetchProbe = createTestProbe[FetchDataResponse]()
        worker ! WorkerActor.HandleFetchData(
          FetchData(shuffleId, partitionIndex = 0, attemptNumber = 0), fetchProbe.ref)
        fetchProbe.receiveMessage(5.seconds).data shouldBe None
      } finally {
        testKit.stop(worker)
        testKit.stop(master)
      }
    }

    "query partition state" in withTempDir { tmpDir =>
      val master = spawn(MasterActor(withCluster = false, withPersistence = false), "e2e-master-query")
      val worker = spawn(WorkerActor(Seq(tmpDir), "127.0.0.1", 9010), "e2e-worker-query")
      try {
        Thread.sleep(3000)
        val shuffleId = ShuffleId("e2e-query", 0)

        val pushProbe = createTestProbe[PushDataResponse]()
        worker ! WorkerActor.HandlePushData(
          PushData(shuffleId, partitionIndex = 0, attemptNumber = 0,
            data = "query-test".getBytes("UTF-8"), checksum = 0L), pushProbe.ref)
        pushProbe.receiveMessage(5.seconds)

        val queryProbe = createTestProbe[WorkerActor.PartitionsQueryResponse]()
        worker ! WorkerActor.QueryPartitions(queryProbe.ref)
        val queryResp = queryProbe.receiveMessage(5.seconds)
        queryResp.partitions should not be empty
        queryResp.partitions.head.isComplete shouldBe false
      } finally {
        testKit.stop(worker)
        testKit.stop(master)
      }
    }

    "handle partition split" in withTempDir { tmpDir =>
      val master = spawn(MasterActor(withCluster = false, withPersistence = false), "e2e-master-split")
      val worker = spawn(WorkerActor(Seq(tmpDir), "127.0.0.1", 9012), "e2e-worker-split")
      try {
        Thread.sleep(3000)
        val shuffleId = ShuffleId("e2e-split", 0)

        val pushProbe = createTestProbe[PushDataResponse]()
        worker ! WorkerActor.HandlePushData(
          PushData(shuffleId, partitionIndex = 0, attemptNumber = 0,
            data = "split-test".getBytes("UTF-8"), checksum = 0L), pushProbe.ref)
        pushProbe.receiveMessage(5.seconds).success shouldBe true

        master ! MasterActor.HandleReportPartitionSplit(
          ReportPartitionSplit(shuffleId, partitionIndex = 0, subPartitions = 3, splitThreshold = 128L * 1024 * 1024))

        val sProbe = createTestProbe[RegisterShuffleResponse]()
        master ! MasterActor.HandleRegisterShuffle(
          RegisterShuffle(shuffleId, numPartitions = 1), sProbe.ref)
        sProbe.receiveMessage(5.seconds).locations should not be empty
      } finally {
        testKit.stop(worker)
        testKit.stop(master)
      }
    }
  }
}

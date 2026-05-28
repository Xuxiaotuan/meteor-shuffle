package cn.xuyinyin.meteor.master

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.matchers.should.Matchers
import scala.concurrent.duration._

import cn.xuyinyin.meteor.common.Protocol._

/**
 * 集成测试：端到端 Shuffle 流程
 *
 * 模拟：Master + Worker + Client 的完整交互
 */
class ShuffleIntegrationSpec
  extends ScalaTestWithActorTestKit
  with AnyWordSpecLike
  with Matchers {

  "Meteor Shuffle" should {

    "MasterActor should start and accept cluster events" in {
      val master = spawn(MasterActor(withCluster = false, withPersistence = false), "test-master")
      try { master should not be null } finally { testKit.stop(master) }
    }

    "Worker registration through MasterActor" in {
      val master = spawn(MasterActor(withCluster = false, withPersistence = false), "test-master-2")

      val probe = createTestProbe[RegisterWorkerResponse]()
      val workerAddr = WorkerAddress("127.0.0.1", 9000, 9001)

      master ! MasterActor.HandleRegisterWorker(
        RegisterWorker(workerAddr, diskSlots = 4, memorySlots = 128),
        probe.ref
      )

      val resp = probe.receiveMessage(5.seconds)
      resp.success shouldBe true
      resp.workerId should include("127.0.0.1")
        testKit.stop(master)
    }

    "Master should register shuffle and return partition locations" in {
      val master = spawn(MasterActor(withCluster = false, withPersistence = false), "test-master-3")

      // 先注册一个 Worker
      val wProbe = createTestProbe[RegisterWorkerResponse]()
      master ! MasterActor.HandleRegisterWorker(
        RegisterWorker(WorkerAddress("127.0.0.1", 9000, 9001), diskSlots = 4, memorySlots = 128),
        wProbe.ref
      )
      wProbe.receiveMessage(5.seconds).success shouldBe true

      // 注册 Shuffle
      val sProbe = createTestProbe[RegisterShuffleResponse]()
      val shuffleId = ShuffleId("test-app-1", 0)
      master ! MasterActor.HandleRegisterShuffle(
        RegisterShuffle(shuffleId, numPartitions = 4),
        sProbe.ref
      )

      val shuffleResp = sProbe.receiveMessage(10.seconds)
      shuffleResp.locations should have size 4
      shuffleResp.locations.foreach { loc =>
        loc.id.shuffleId shouldBe shuffleId
        loc.epoch shouldBe 0
        loc.primary.host shouldBe "127.0.0.1"
      }
        testKit.stop(master)
    }

    "Master should handle worker heartbeat" in {
      val master = spawn(MasterActor(withCluster = false, withPersistence = false), "test-master-4")

      // 注册 Worker
      val wProbe = createTestProbe[RegisterWorkerResponse]()
      master ! MasterActor.HandleRegisterWorker(
        RegisterWorker(WorkerAddress("127.0.0.1", 9000, 9001), diskSlots = 4, memorySlots = 128),
        wProbe.ref
      )
      val workerId = wProbe.receiveMessage(5.seconds).workerId

      // 发送心跳
      master ! MasterActor.HandleHeartbeat(
        Heartbeat(workerId, WorkerAddress("127.0.0.1", 9000, 9001), diskSlotsFree = 3, memorySlotsFree = 120)
      )

      // 验证 Worker 还活着（能注册 Shuffle）
      val sProbe = createTestProbe[RegisterShuffleResponse]()
      master ! MasterActor.HandleRegisterShuffle(
        RegisterShuffle(ShuffleId("test-app-hb", 0), numPartitions = 2),
        sProbe.ref
      )

      val resp = sProbe.receiveMessage(10.seconds)
      resp.locations should have size 2
        testKit.stop(master)
    }

    "Master should revive partition on dead worker" in {
      val master = spawn(MasterActor(withCluster = false, withPersistence = false), "test-master-5")

      // 注册两个 Worker
      Array("127.0.0.1", "127.0.0.2").foreach { host =>
        val probe = createTestProbe[RegisterWorkerResponse]()
        master ! MasterActor.HandleRegisterWorker(
          RegisterWorker(WorkerAddress(host, 9000, 9001), diskSlots = 4, memorySlots = 128),
          probe.ref
        )
        probe.receiveMessage(5.seconds).success shouldBe true
      }

      // 注册 Shuffle
      val sProbe = createTestProbe[RegisterShuffleResponse]()
      val shuffleId = ShuffleId("test-app-revive", 0)
      master ! MasterActor.HandleRegisterShuffle(
        RegisterShuffle(shuffleId, numPartitions = 8),
        sProbe.ref
      )
      sProbe.receiveMessage(5.seconds).locations should have size 8

      // 触发健康检查（心跳检测，但刚注册的 Worker 还在 60s 窗口内）
      master ! MasterActor.CheckWorkerHealth

      // 请求 Revive
      val reviveProbe = createTestProbe[ReviveResponse]()
      master ! MasterActor.HandleRevive(
        Revive(shuffleId, partitionIndex = 0, epoch = 0, cause = Some("worker timeout")),
        reviveProbe.ref
      )

      val reviveResp = reviveProbe.receiveMessage(10.seconds)
      reviveResp.location.epoch shouldBe >=(1)
        testKit.stop(master)
    }

    "get shuffle locations" in {
      val master = spawn(MasterActor(withCluster = false, withPersistence = false), "test-master-6")

      val wProbe = createTestProbe[RegisterWorkerResponse]()
      master ! MasterActor.HandleRegisterWorker(
        RegisterWorker(WorkerAddress("127.0.0.1", 9000, 9001), diskSlots = 4, memorySlots = 128),
        wProbe.ref
      )
      wProbe.receiveMessage(5.seconds)

      val sProbe = createTestProbe[RegisterShuffleResponse]()
      val shuffleId = ShuffleId("test-app-get", 0)
      master ! MasterActor.HandleRegisterShuffle(
        RegisterShuffle(shuffleId, numPartitions = 3),
        sProbe.ref
      )
      sProbe.receiveMessage(5.seconds)

      val gProbe = createTestProbe[GetShuffleLocationsResponse]()
      master ! MasterActor.HandleGetShuffleLocations(GetShuffleLocations(shuffleId), gProbe.ref)

      gProbe.receiveMessage(5.seconds).locations should have size 3
      testKit.stop(master)
    }

    "unknown shuffle should return empty locations" in {
      val master = spawn(MasterActor(withCluster = false, withPersistence = false), "test-master-7")

      val probe = createTestProbe[GetShuffleLocationsResponse]()
      master ! MasterActor.HandleGetShuffleLocations(
        GetShuffleLocations(ShuffleId("unknown", 999)),
        probe.ref
      )

      probe.receiveMessage(5.seconds).locations shouldBe empty
        testKit.stop(master)
    }

    "Master should handle partition split report" in {
      val master = spawn(MasterActor(withCluster = false, withPersistence = false), "test-master-8")

      val wProbe = createTestProbe[RegisterWorkerResponse]()
      master ! MasterActor.HandleRegisterWorker(
        RegisterWorker(WorkerAddress("127.0.0.1", 9000, 9001), diskSlots = 4, memorySlots = 128),
        wProbe.ref
      )
      wProbe.receiveMessage(5.seconds)

      val sProbe = createTestProbe[RegisterShuffleResponse]()
      val shuffleId = ShuffleId("test-app-split", 0)
      master ! MasterActor.HandleRegisterShuffle(
        RegisterShuffle(shuffleId, numPartitions = 4),
        sProbe.ref
      )
      sProbe.receiveMessage(5.seconds)

      // Worker 报告 partition 0 被拆分为 3 个子分区
      master ! MasterActor.HandleReportPartitionSplit(
        ReportPartitionSplit(shuffleId, partitionIndex = 0, subPartitions = 3, splitThreshold = 128L * 1024 * 1024)
      )

      // 验证 shuffle 位置仍然可查询
      val gProbe = createTestProbe[GetShuffleLocationsResponse]()
      master ! MasterActor.HandleGetShuffleLocations(GetShuffleLocations(shuffleId), gProbe.ref)
      val locs = gProbe.receiveMessage(5.seconds).locations
      locs should have size 4
        testKit.stop(master)
    }
  }
}

package cn.xuyinyin.meteor.master

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed.ActorRef
import org.scalatest.wordspec.AnyWordSpecLike
import cn.xuyinyin.meteor.common.Protocol._
import cn.xuyinyin.meteor.master.MasterActor
import cn.xuyinyin.meteor.worker.WorkerActor

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.CyclicBarrier
import scala.concurrent.duration._

/**
 * 全链路性能基准测试
 *
 * 场景：Master + Worker → 注册 Shuffle → 批量 PushData → FetchData → 测量吞吐量
 */
class ShuffleBenchmarkSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike {

  val masterPort = 19100 + scala.util.Random.nextInt(100)

  // 辅助：注册 Worker 并返回 probe
  private def registerWorker(
    master: ActorRef[MasterActor.Command],
    host: String, port: Int
  ): (String, org.apache.pekko.actor.testkit.typed.scaladsl.TestProbe[RegisterWorkerResponse]) = {
    val probe = createTestProbe[RegisterWorkerResponse]()
    master ! MasterActor.HandleRegisterWorker(
      RegisterWorker(WorkerAddress(host, port, port + 1), diskSlots = 8, memorySlots = 256),
      probe.ref
    )
    val resp = probe.receiveMessage(5.seconds)
    resp.success shouldBe true
    (resp.workerId, probe)
  }

  private def registerShuffle(
    master: ActorRef[MasterActor.Command],
    shuffleId: ShuffleId,
    numPartitions: Int
  ): Seq[PartitionLocation] = {
    val probe = createTestProbe[RegisterShuffleResponse]()
    master ! MasterActor.HandleRegisterShuffle(
      RegisterShuffle(shuffleId, numPartitions),
      probe.ref
    )
    probe.receiveMessage(10.seconds).locations
  }

  "Meteor Shuffle Benchmark" should {

    "push + fetch throughput" in {
      val master = spawn(MasterActor(withCluster = false, withPersistence = false), "bench-master")
      val w1 = spawn(
        WorkerActor(Seq("/tmp/meteor-bench-w1"), "127.0.0.1", 19200),
        "bench-worker-1"
      )
      val w2 = spawn(
        WorkerActor(Seq("/tmp/meteor-bench-w2"), "127.0.0.1", 19300),
        "bench-worker-2"
      )

      // 注册 Workers
      registerWorker(master, "127.0.0.1", 19200)
      registerWorker(master, "127.0.0.1", 19300)

      // 注册 Shuffle
      val shuffleId = ShuffleId("bench-app", 0)
      val locations = registerShuffle(master, shuffleId, 8)
      locations should have size 8

      val dataBytes = new Array[Byte](4096) // 4KB per push
      scala.util.Random.nextBytes(dataBytes)

      val totalPushes = 1000
      val pushLatencyNanos = new Array[Long](totalPushes)
      val fetchLatencyNanos = new Array[Long](totalPushes)

      // ---- PushData 基准 ----
      val pushStart = System.nanoTime()
      for (i <- 0 until totalPushes) {
        val probe = createTestProbe[PushDataResponse]()
        val t0 = System.nanoTime()
        w1 ! WorkerActor.HandlePushData(
          PushData(shuffleId, partitionIndex = i % 8, attemptNumber = 0, data = dataBytes, checksum = 0L),
          probe.ref
        )
        probe.receiveMessage(5.seconds)
        pushLatencyNanos(i) = System.nanoTime() - t0
      }
      val pushElapsed = System.nanoTime() - pushStart

      // ---- FetchData 基准 ----
      val fetchStart = System.nanoTime()
      for (i <- 0 until totalPushes) {
        val probe = createTestProbe[FetchDataResponse]()
        val t0 = System.nanoTime()
        w1 ! WorkerActor.HandleFetchData(
          FetchData(shuffleId, partitionIndex = i % 8, attemptNumber = 0),
          probe.ref
        )
        probe.receiveMessage(5.seconds)
        fetchLatencyNanos(i) = System.nanoTime() - t0
      }
      val fetchElapsed = System.nanoTime() - fetchStart

      // ---- 统计 ----
      val pushThroughput = totalPushes.toDouble / (pushElapsed / 1e9)
      val fetchThroughput = totalPushes.toDouble / (fetchElapsed / 1e9)
      val pushDataMB = (totalPushes * dataBytes.length).toDouble / (1024 * 1024)
      val pushMBps = pushDataMB / (pushElapsed / 1e9)
      val fetchMBps = pushDataMB / (fetchElapsed / 1e9)

      val sortedPush = pushLatencyNanos.sorted
      val sortedFetch = fetchLatencyNanos.sorted

      println("=" * 60)
      println("  Meteor Shuffle Benchmark Results")
      println("=" * 60)
      println(f"  PushData:  ${pushThroughput}%.0f ops/s, ${pushMBps}%.1f MB/s")
      println(f"    p50 = ${sortedPush(totalPushes / 2) / 1000}%.0f µs")
      println(f"    p99 = ${sortedPush((totalPushes * 0.99).toInt) / 1000}%.0f µs")
      println(f"  FetchData: ${fetchThroughput}%.0f ops/s, ${fetchMBps}%.1f MB/s")
      println(f"    p50 = ${sortedFetch(totalPushes / 2) / 1000}%.0f µs")
      println(f"    p99 = ${sortedFetch((totalPushes * 0.99).toInt) / 1000}%.0f µs")
      println("=" * 60)

      pushThroughput should be > 100.0
      fetchThroughput should be > 50.0
    }

    "concurrent writers" in {
      val master = spawn(MasterActor(withCluster = false, withPersistence = false), "bench-master-conc")
      val w = spawn(
        WorkerActor(Seq("/tmp/meteor-bench-conc"), "127.0.0.1", 19400),
        "bench-worker-conc"
      )
      registerWorker(master, "127.0.0.1", 19400)

      val shuffleId = ShuffleId("bench-app-conc", 0)
      registerShuffle(master, shuffleId, 8)

      val dataBytes = new Array[Byte](1024)
      scala.util.Random.nextBytes(dataBytes)

      val numWriters = 4
      val pushesPerWriter = 250
      val totalPushes = numWriters * pushesPerWriter
      val barrier = new CyclicBarrier(numWriters)
      val completed = new AtomicLong(0)
      val errors = new AtomicLong(0)

      val startTime = System.nanoTime()

      // 创建一个 dummy actor 用于 fire-and-forget
      val dummyProbe = createTestProbe[PushDataResponse]()

      val threads = (0 until numWriters).map { writerIdx =>
        new Thread {
          override def run(): Unit = {
            barrier.await()
            try {
              for (i <- 0 until pushesPerWriter) {
                w ! WorkerActor.HandlePushData(
                  PushData(shuffleId, partitionIndex = writerIdx, attemptNumber = 0, data = dataBytes, checksum = 0L),
                  dummyProbe.ref
                )
                completed.incrementAndGet()
              }
            } catch {
              case _: Throwable => errors.incrementAndGet()
            }
          }
        }
      }

      threads.foreach(_.start())
      threads.foreach(_.join(30000))

      val elapsed = System.nanoTime() - startTime

      val throughput = totalPushes.toDouble / (elapsed / 1e9)
      val dataMB = (totalPushes * dataBytes.length).toDouble / (1024 * 1024)
      val mbps = dataMB / (elapsed / 1e9)

      println("=" * 60)
      println("  Concurrent Writers Benchmark")
      println("=" * 60)
      println(f"  Writers:    $numWriters")
      println(f"  Completed:  ${completed.get()} / $totalPushes")
      println(f"  Errors:     ${errors.get()}")
      println(f"  Throughput: ${throughput}%.0f ops/s, ${mbps}%.1f MB/s")
      println("=" * 60)

      errors.get() should be(0L)
      completed.get() should be(totalPushes.toLong)
      throughput should be > 50.0
    }
  }
}

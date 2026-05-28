package cn.xuyinyin.meteor.worker

import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors

import java.nio.file.{Files, Path}
import scala.concurrent.duration._

/**
 * Worker 磁盘监控 + 水位线 Eviction
 *
 * 对标 Celeborn 的 StorageManager 磁盘监控：
 *   - 定期检查存储目录的磁盘使用率
 *   - 超过高水位线（highWatermark）触发 eviction
 *   - 驱逐到低水位线（lowWatermark）停止
 *   - 使用 LRU 策略驱逐已完成的 partition
 *
 * 配置：
 *   meteor.worker.storage.high-watermark = 0.85  (85%)
 *   meteor.worker.storage.low-watermark  = 0.70  (70%)
 *   meteor.worker.storage.check-interval = 30s
 */
object DiskMonitor {

  sealed trait Command

  // ===== 对外消息 =====
  final case class CheckNow(replyTo: ActorRef[DiskStatus]) extends Command
  final case class DiskStatus(
    totalSpace: Long,
    usedSpace: Long,
    freeSpace: Long,
    usageRatio: Double,
    isHighWatermark: Boolean,
    storageDirs: Seq[DirStatus]
  )
  final case class DirStatus(path: String, totalSpace: Long, freeSpace: Long, usageRatio: Double)

  // ===== 内部 =====
  private case object Tick extends Command

  // Eviction callback: 当需要驱逐时，通知 WorkerActor
  final case class EvictionRequired(count: Int) extends Command

  def apply(
    storageDirs: Seq[String],
    highWatermark: Double = 0.85,
    lowWatermark: Double = 0.70,
    checkInterval: FiniteDuration = 30.seconds,
    workerRef: ActorRef[WorkerActor.Command],
    statusRef: ActorRef[DiskStatus] = null
  ): Behavior[Command] = Behaviors.setup { ctx =>
    ctx.log.info(s"DiskMonitor starting: dirs=${storageDirs.mkString(",")}, " +
      s"high=${(highWatermark * 100).toInt}%, low=${(lowWatermark * 100).toInt}%, interval=$checkInterval")

    // 定时检查
    Behaviors.withTimers { timers =>
      timers.startTimerWithFixedDelay(Tick, checkInterval)

      // 首次立即检查
      ctx.self ! Tick

      Behaviors.receiveMessage {
        case Tick =>
          val status = checkDisk(storageDirs)

          // 推送状态到 WorkerActor（用于 free-space-aware 选盘）
          if (statusRef != null) {
            statusRef ! status
          }

          if (status.isHighWatermark) {
            ctx.log.warn(s"Disk HIGH WATERMARK: usage=${(status.usageRatio * 100).toInt}%, " +
              s"free=${formatBytes(status.freeSpace)}/${formatBytes(status.totalSpace)}")
            // 通知 WorkerActor 需要驱逐
            val targetFree = (status.totalSpace * (1.0 - lowWatermark)).toLong
            val bytesToEvict = targetFree - status.freeSpace
            if (bytesToEvict > 0) {
              ctx.log.info(s"Need to evict ~${formatBytes(bytesToEvict)} to reach low watermark")
              workerRef ! WorkerActor.EvictPartitions(bytesToEvict)
            }
          }
          Behaviors.same

        case CheckNow(replyTo) =>
          val status = checkDisk(storageDirs)
          replyTo ! status
          Behaviors.same

        case _: EvictionRequired =>
          Behaviors.same  // 由 WorkerActor 处理
      }
    }
  }

  private def checkDisk(storageDirs: Seq[String]): DiskStatus = {
    val dirStatuses = storageDirs.map { dirStr =>
      val path = java.nio.file.Paths.get(dirStr)
      if (Files.exists(path)) {
        val store = path.getFileSystem.getFileStores.iterator()
        var total = 0L
        var free = 0L
        while (store.hasNext) {
          val fs = store.next()
          total += fs.getTotalSpace
          free += fs.getUsableSpace
        }
        val used = total - free
        DirStatus(dirStr, total, free, if (total > 0) used.toDouble / total else 0.0)
      } else {
        DirStatus(dirStr, 0, 0, 0.0)
      }
    }

    val totalSpace = dirStatuses.map(_.totalSpace).sum
    val freeSpace = dirStatuses.map(_.freeSpace).sum
    val usedSpace = totalSpace - freeSpace
    val usageRatio = if (totalSpace > 0) usedSpace.toDouble / totalSpace else 0.0

    DiskStatus(
      totalSpace = totalSpace,
      usedSpace = usedSpace,
      freeSpace = freeSpace,
      usageRatio = usageRatio,
      isHighWatermark = usageRatio > 0.85,  // 默认高水位线
      storageDirs = dirStatuses
    )
  }

  private def formatBytes(bytes: Long): String = {
    if (bytes >= 1024L * 1024 * 1024) f"${bytes / 1024.0 / 1024 / 1024}%.1f GB"
    else if (bytes >= 1024L * 1024) f"${bytes / 1024.0 / 1024}%.1f MB"
    else if (bytes >= 1024L) f"${bytes / 1024.0}%.1f KB"
    else s"${bytes} B"
  }
}

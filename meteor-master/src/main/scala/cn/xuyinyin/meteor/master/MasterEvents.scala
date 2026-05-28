package cn.xuyinyin.meteor.master

import cn.xuyinyin.meteor.common.Protocol._

/**
 * Master Event Sourcing 事件
 *
 * 对标 Celeborn Master 的 Raft State Machine 日志。
 * 使用 Pekko Persistence + LevelDB 持久化。
 */
object MasterEvents {

  /** 核心状态（从事件流重建） */
  final case class MasterState(
    workers: Map[String, WorkerInfo] = Map.empty,
    shuffles: Map[ShuffleId, ShuffleInfo] = Map.empty,
    committedPartitions: Map[ShuffleId, Set[Int]] = Map.empty,  // shuffleId -> committed partition indices
    excludedWorkers: Set[String] = Set.empty,                    // blacklisted worker ids
    workerFailureCounts: Map[String, Int] = Map.empty,           // workerId -> consecutive failures
    shuttingDown: Boolean = false,                               // 优雅关闭标志
    shuffleRegisterTimestamps: Map[ShuffleId, Long] = Map.empty  // shuffle注册时间戳（用于过期清理）
  )

  final case class WorkerInfo(
    workerId: String,
    address: WorkerAddress,
    diskSlotsFree: Int,
    memorySlotsFree: Int,
    lastHeartbeatMs: Long = System.currentTimeMillis(),
    isExcluded: Boolean = false
  ) {
    def isAlive: Boolean = System.currentTimeMillis() - lastHeartbeatMs < 60000
  }

  final case class ShuffleInfo(
    shuffleId: ShuffleId,
    partitions: Map[Int, PartitionLocation],
    subPartitionCounts: Map[Int, Int] = Map.empty    // partitionIndex -> 子分区数（split 后）
  )

  // ================================
  // 事件定义
  // ================================

  sealed trait Event

  final case class WorkerRegistered(
    workerId: String,
    address: WorkerAddress,
    diskSlots: Int,
    memorySlots: Int
  ) extends Event

  final case class WorkerHeartbeat(
    workerId: String,
    diskSlotsFree: Int,
    memorySlotsFree: Int,
    timestamp: Long
  ) extends Event

  final case class WorkerTimedOut(
    workerId: String,
    atTimestamp: Long
  ) extends Event

  final case class ShuffleRegistered(
    shuffleId: ShuffleId,
    numPartitions: Int,
    locations: Seq[PartitionLocation]
  ) extends Event

  final case class ShuffleRevived(
    shuffleId: ShuffleId,
    partitionIndex: Int,
    oldEpoch: Int,
    newEpoch: Int,
    newLocation: PartitionLocation
  ) extends Event

  final case class ShuffleUnregistered(
    shuffleId: ShuffleId
  ) extends Event

  final case class PartitionSplit(
    shuffleId: ShuffleId,
    partitionIndex: Int,
    subPartitions: Int
  ) extends Event

  final case class PartitionCommitRecorded(
    shuffleId: ShuffleId,
    partitionIndex: Int,
    workerId: String,
    dataSize: Long
  ) extends Event

  final case class WorkerExcluded(
    workerId: String,
    reason: String
  ) extends Event

  final case class WorkerReinstated(
    workerId: String
  ) extends Event

  case object ShuttingDown extends Event

  // ================================
  // Event Sourcing 状态机
  // ================================

  /** 应用事件到状态 */
  def applyEvent(state: MasterState, event: Event): MasterState = event match {

    case WorkerRegistered(id, addr, disk, mem) =>
      val winfo = WorkerInfo(id, addr, disk, mem)
      state.copy(workers = state.workers + (id -> winfo))

    case WorkerHeartbeat(id, diskFree, memFree, ts) =>
      state.workers.get(id) match {
        case Some(info) =>
          val updated = info.copy(diskSlotsFree = diskFree, memorySlotsFree = memFree, lastHeartbeatMs = ts)
          state.copy(workers = state.workers + (id -> updated))
        case None => state
      }

    case WorkerTimedOut(id, _) =>
      val removed = state.workers.get(id)
      val remainingShuffles = removed match {
        case Some(info) =>
          state.shuffles.map { case (sid, si) =>
            sid -> si.copy(partitions = si.partitions.filterNot {
              case (_, loc) => loc.primary == info.address
            })
          }
        case None => state.shuffles
      }
      state.copy(workers = state.workers - id, shuffles = remainingShuffles)

    case ShuffleRegistered(sid, _, locs) =>
      val partitions = locs.map(l => l.id.partitionIndex -> l).toMap
      state.copy(
        shuffles = state.shuffles + (sid -> ShuffleInfo(sid, partitions)),
        shuffleRegisterTimestamps = state.shuffleRegisterTimestamps + (sid -> System.currentTimeMillis())
      )

    case ShuffleRevived(sid, pidx, _, _, newLoc) =>
      state.shuffles.get(sid) match {
        case Some(si) =>
          val updated = si.copy(partitions = si.partitions + (pidx -> newLoc))
          state.copy(shuffles = state.shuffles + (sid -> updated))
        case None => state
      }

    case ShuffleUnregistered(sid) =>
      state.copy(
        shuffles = state.shuffles - sid,
        committedPartitions = state.committedPartitions - sid,
        shuffleRegisterTimestamps = state.shuffleRegisterTimestamps - sid
      )

    case PartitionSplit(sid, pidx, count) =>
      state.shuffles.get(sid) match {
        case Some(si) =>
          val updated = si.copy(subPartitionCounts = si.subPartitionCounts + (pidx -> count))
          state.copy(shuffles = state.shuffles + (sid -> updated))
        case None => state
      }

    case PartitionCommitRecorded(sid, pidx, _, _) =>
      val existing = state.committedPartitions.getOrElse(sid, Set.empty)
      state.copy(committedPartitions = state.committedPartitions + (sid -> (existing + pidx)))

    case WorkerExcluded(workerId, reason) =>
      state.workers.get(workerId) match {
        case Some(info) =>
          val updated = info.copy(isExcluded = true)
          state.copy(
            workers = state.workers + (workerId -> updated),
            excludedWorkers = state.excludedWorkers + workerId
          )
        case None => state
      }

    case WorkerReinstated(workerId) =>
      state.workers.get(workerId) match {
        case Some(info) =>
          val updated = info.copy(isExcluded = false)
          state.copy(
            workers = state.workers + (workerId -> updated),
            excludedWorkers = state.excludedWorkers - workerId,
            workerFailureCounts = state.workerFailureCounts - workerId
          )
        case None => state
      }

    case ShuttingDown =>
      state.copy(shuttingDown = true)
  }
}

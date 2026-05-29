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
    shuffleRegisterTimestamps: Map[ShuffleId, Long] = Map.empty,  // shuffle注册时间戳（用于过期清理）
    // 两阶段提交追踪
    shuffleNumMappers: Map[ShuffleId, Int] = Map.empty,          // shuffle → 总 mapper 数
    mapperCompletions: Map[ShuffleId, Set[Int]] = Map.empty,     // shuffle → 已完成的 mapper id 集合
    shuffleCommitted: Set[ShuffleId] = Set.empty                  // 已完成 CommitShuffle 的 shuffle
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
    locations: Seq[PartitionLocation],
    numMappers: Int = 1   // 用于两阶段提交追踪
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

  /** 两阶段提交：单个 mapper 完成 */
  final case class MapperCompleted(
    shuffleId: ShuffleId,
    mapperId: Int
  ) extends Event

  /** 两阶段提交：所有 mapper 完成，CommitShuffle 已广播 */
  final case class ShuffleCommitted(
    shuffleId: ShuffleId
  ) extends Event

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

    case ShuffleRegistered(sid, _, locs, nMappers) =>
      val partitions = locs.map(l => l.id.partitionIndex -> l).toMap
      state.copy(
        shuffles = state.shuffles + (sid -> ShuffleInfo(sid, partitions)),
        shuffleRegisterTimestamps = state.shuffleRegisterTimestamps + (sid -> System.currentTimeMillis()),
        shuffleNumMappers = state.shuffleNumMappers + (sid -> nMappers)
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
        shuffleRegisterTimestamps = state.shuffleRegisterTimestamps - sid,
        shuffleNumMappers = state.shuffleNumMappers - sid,
        mapperCompletions = state.mapperCompletions - sid,
        shuffleCommitted = state.shuffleCommitted - sid
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

    case MapperCompleted(sid, mapperId) =>
      val existing = state.mapperCompletions.getOrElse(sid, Set.empty)
      val newDone = existing + mapperId
      val totalMappers = state.shuffleNumMappers.getOrElse(sid, 1)
      val s1 = state.copy(mapperCompletions = state.mapperCompletions + (sid -> newDone))
      // 所有 mapper 完成 → 自动标记 shuffle committed
      if (newDone.size >= totalMappers) s1.copy(shuffleCommitted = s1.shuffleCommitted + sid)
      else s1

    case ShuffleCommitted(sid) =>
      state.copy(shuffleCommitted = state.shuffleCommitted + sid)
  }
}

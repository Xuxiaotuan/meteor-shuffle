package cn.xuyinyin.meteor.master

import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.receptionist.Receptionist
import org.apache.pekko.cluster.ClusterEvent._
import org.apache.pekko.cluster.typed.{Cluster, Subscribe}
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import scala.concurrent.duration._

import cn.xuyinyin.meteor.common.Protocol._
import cn.xuyinyin.meteor.common.{ServiceKeys, MasterActorCommand, Metrics}
import MasterEvents._

/**
 * Meteor Master — 控制面核心（Event Sourcing 版）
 *
 * 状态变化通过 Pekko Persistence 持久化到 LevelDB。
 * 重启后从事件流重建完整的 Worker/Shuffle 状态。
 *
 * 对标 Celeborn Master (Raft 3 节点)
 * 替代：Pekko Cluster + Persistence → 自动 HA + 持久化，无需手撸 Raft
 */
object MasterActor {

  sealed trait Command

  // ----- 集群事件 -----
  private final case class WrappedClusterEvent(event: ClusterDomainEvent) extends Command

  // ----- Worker 管理 -----
  final case class HandleRegisterWorker(req: RegisterWorker, replyTo: ActorRef[RegisterWorkerResponse]) extends Command
  final case class HandleHeartbeat(req: Heartbeat) extends Command
  final case class HandleUnregisterWorker(req: UnregisterWorker) extends Command
  case object CheckWorkerHealth extends Command
  final case class WorkerTimeout(workerId: String) extends Command

  // ----- Shuffle 管理 -----
  final case class HandleRegisterShuffle(req: RegisterShuffle, replyTo: ActorRef[RegisterShuffleResponse]) extends Command
  final case class HandleRevive(req: Revive, replyTo: ActorRef[ReviveResponse]) extends Command
  final case class HandleGetShuffleLocations(req: GetShuffleLocations, replyTo: ActorRef[GetShuffleLocationsResponse]) extends Command
  final case class HandleReleaseSlots(req: ReleaseSlots) extends Command
  final case class HandleUnregisterShuffle(req: UnregisterShuffle) extends Command
  final case class HandleReportPartitionSplit(req: ReportPartitionSplit) extends Command
  final case class HandlePartitionCommitted(req: cn.xuyinyin.meteor.common.Protocol.PartitionCommitted) extends Command
  final case class HandleIsPartitionReady(req: IsPartitionReady, replyTo: ActorRef[IsPartitionReadyResponse]) extends Command
  final case class HandleReportWorkerFailure(req: ReportWorkerFailure) extends Command
  final case class HandleMapperEnd(req: MapperEnd, replyTo: ActorRef[MapperEndResponse]) extends Command
  final case class HandleCommitShuffleResponse(req: CommitShuffleResponse) extends Command

  // ----- 状态查询 -----
  final case class QueryWorkers(replyTo: ActorRef[WorkersQueryResponse]) extends Command
  final case class QueryShuffles(replyTo: ActorRef[ShufflesQueryResponse]) extends Command

  // ================================
  // 查询响应
  // ================================
  final case class WorkerStateInfo(
    workerId: String,
    host: String,
    port: Int,
    diskSlotsFree: Int,
    memorySlotsFree: Int,
    lastHeartbeatMs: Long,
    isAlive: Boolean
  )
  final case class WorkersQueryResponse(workers: Seq[WorkerStateInfo])

  final case class ShuffleStateInfo(
    appId: String,
    shuffleNum: Int,
    numPartitions: Int,
    subPartitionCounts: Map[Int, Int]
  )
  final case class ShufflesQueryResponse(shuffles: Seq[ShuffleStateInfo])

  // ===== 优雅关闭 =====
  case object GracefulShutdown extends Command

  // ===== 定时清理 =====
  case object CheckShuffleCleanup extends Command

  // ================================
  // 工厂方法
  // ================================

  /** 生产环境：启用 Pekko Cluster + Persistence */
  def apply(): Behavior[Command] = apply(withCluster = true, withPersistence = true)

  /** 测试/单节点模式 */
  def apply(withCluster: Boolean, withPersistence: Boolean = false): Behavior[Command] = Behaviors.setup[Command] { ctx =>
    if (withCluster) {
      val cluster = Cluster(ctx.system)
      val clusterAdapter = ctx.messageAdapter(WrappedClusterEvent.apply)
      cluster.subscriptions ! Subscribe(clusterAdapter, classOf[ClusterDomainEvent])
    }

    // 注册到 Receptionist（所有消息透传，replyTo 由具体方法携带）
    ctx.system.receptionist ! Receptionist.Register(
      ServiceKeys.MasterService,
      ctx.messageAdapter[MasterActorCommand] {
        case MasterActorCommand.WrappedRegisterWorker(rw, replyTo) =>
          HandleRegisterWorker(rw, replyTo.asInstanceOf[org.apache.pekko.actor.typed.ActorRef[RegisterWorkerResponse]])
        case MasterActorCommand.WrappedHeartbeat(hb) => HandleHeartbeat(hb)
        case MasterActorCommand.WrappedUnregisterWorker(uw) => HandleUnregisterWorker(uw)
        case MasterActorCommand.WrappedReportPartitionSplit(rps) => HandleReportPartitionSplit(rps)
        case MasterActorCommand.WrappedRegisterShuffle(rs, replyTo) =>
          HandleRegisterShuffle(rs, replyTo.asInstanceOf[org.apache.pekko.actor.typed.ActorRef[RegisterShuffleResponse]])
        case MasterActorCommand.WrappedRevive(r, replyTo) =>
          HandleRevive(r, replyTo.asInstanceOf[org.apache.pekko.actor.typed.ActorRef[ReviveResponse]])
        case MasterActorCommand.WrappedGetLocations(gl, replyTo) =>
          HandleGetShuffleLocations(gl, replyTo.asInstanceOf[org.apache.pekko.actor.typed.ActorRef[GetShuffleLocationsResponse]])
        case MasterActorCommand.WrappedPartitionCommitted(pc) =>
          HandlePartitionCommitted(pc)
        case MasterActorCommand.WrappedIsPartitionReady(ipr, replyTo) =>
          HandleIsPartitionReady(ipr, replyTo.asInstanceOf[org.apache.pekko.actor.typed.ActorRef[IsPartitionReadyResponse]])
        case MasterActorCommand.WrappedReportWorkerFailure(rwf) =>
          HandleReportWorkerFailure(rwf)
        case MasterActorCommand.WrappedMapperEnd(me, replyTo) =>
          HandleMapperEnd(me, replyTo.asInstanceOf[org.apache.pekko.actor.typed.ActorRef[MapperEndResponse]])
        case MasterActorCommand.WrappedCommitShuffleResponse(csr) =>
          HandleCommitShuffleResponse(csr)
        case _ => ???
      }
    )

    // 启动心跳检测
    ctx.scheduleOnce(30.seconds, ctx.self, CheckWorkerHealth)

    // 启动 Shuffle 数据清理（每 5 分钟）
    ctx.scheduleOnce(300.seconds, ctx.self, CheckShuffleCleanup)

    if (withPersistence) {
      val esb = EventSourcedBehavior[Command, Event, MasterState](
        persistenceId = PersistenceId.ofUniqueId("meteor-master"),
        emptyState = MasterState(),
        commandHandler = commandHandler(ctx),
        eventHandler = (state, event) => applyEvent(state, event)
      )
      esb
    } else {
      // 无持久化模式（测试用）：本地状态机
      val handlerFn = handleCommand(ctx)
      var state = MasterState()
      Behaviors.receiveMessage { cmd =>
        state = handlerFn(state, cmd)
        Behaviors.same
      }
    }
  }.narrow[Command]

  // ================================
  // Command Handler（纯函数）
  // ================================

  private def commandHandler(ctx: ActorContext[Command])
    : (MasterState, Command) => Effect[Event, MasterState] = { (state, cmd) =>

    cmd match {

      // ==================== 集群事件 ====================
      case WrappedClusterEvent(MemberUp(member)) =>
        ctx.log.info(s"Cluster member UP: ${member.address}")
        Effect.none

      case WrappedClusterEvent(MemberRemoved(member, _)) =>
        ctx.log.warn(s"Cluster member REMOVED: ${member.address}")
        val deadWorkers = state.workers.filter { case (_, w) =>
          member.address.host.exists(h => w.address.host == h)
        }
        deadWorkers.foreach { case (id, _) =>
          ctx.self ! WorkerTimeout(id)
        }
        Effect.none

      case WrappedClusterEvent(_) =>
        Effect.none

      // ==================== Worker 注册 ====================
      case HandleRegisterWorker(req, replyTo) =>
        val workerId = s"${req.address.host}:${req.address.port}"
        state.workers.get(workerId) match {
          case Some(existing) if existing.isAlive =>
            replyTo ! RegisterWorkerResponse(success = true, workerId)
            Effect.none
          case _ =>
            val event = WorkerRegistered(workerId, req.address, req.diskSlots, req.memorySlots)
            ctx.log.info(s"Worker registered: $workerId (disk=${req.diskSlots}, mem=${req.memorySlots})")
            Metrics.MasterMetrics.workersTotal.inc()
            Metrics.MasterMetrics.workersAlive.inc()
            Effect.persist(event).thenRun { _ =>
              replyTo ! RegisterWorkerResponse(success = true, workerId)
            }
        }

      // ==================== Worker 心跳 ====================
      case HandleHeartbeat(req) =>
        val event = WorkerHeartbeat(req.workerId, req.diskSlotsFree, req.memorySlotsFree, System.currentTimeMillis())
        Effect.persist(event)

      // ==================== Worker 注销 ====================
      case HandleUnregisterWorker(req) =>
        val event = WorkerTimedOut(req.workerId, System.currentTimeMillis())
        Effect.persist(event).thenRun { _ =>
          ctx.log.info(s"Worker unregistered: ${req.workerId}")
        }

      // ==================== Worker 超时 ====================
      case WorkerTimeout(workerId) =>
        state.workers.get(workerId) match {
          case Some(w) if !w.isAlive =>
            val event = WorkerTimedOut(workerId, System.currentTimeMillis())
            ctx.log.warn(s"Worker timeout: $workerId, releasing slots")
            Metrics.MasterMetrics.workersAlive.dec()
            Effect.persist(event)
          case _ =>
            Effect.none
        }

      // ==================== 健康检查 ====================
      case CheckWorkerHealth =>
        val dead = state.workers.filter { case (_, w) => !w.isAlive }
        dead.keys.foreach { id => ctx.self ! WorkerTimeout(id) }
        ctx.scheduleOnce(30.seconds, ctx.self, CheckWorkerHealth)
        Effect.none

      // ==================== 注册 Shuffle ====================
      case HandleRegisterShuffle(req, replyTo) =>
        if (state.shuttingDown) {
          ctx.log.warn(s"Master is shutting down, rejecting shuffle registration: ${req.shuffleId}")
          replyTo ! RegisterShuffleResponse(Seq.empty)
          Effect.none
        } else {
          val aliveWorkers = state.workers.filter { case (_, w) => w.isAlive && !state.excludedWorkers.contains(w.workerId) }
          if (aliveWorkers.isEmpty) {
            replyTo ! RegisterShuffleResponse(Seq.empty)
            Effect.none
          } else {
            val workerList = aliveWorkers.values.toSeq
            val locations = (0 until req.numPartitions).map { i =>
              val w = workerList(i % workerList.size)
              PartitionLocation(
                id = PartitionId(req.shuffleId, i),
                epoch = 0,
                primary = w.address,
                replica = None
              )
            }

            val event = ShuffleRegistered(req.shuffleId, req.numPartitions, locations, req.numMappers)
            Metrics.MasterMetrics.shuffleRegistrations.inc()
            Metrics.MasterMetrics.shufflesActive.inc()
            ctx.log.info(s"Shuffle registered: ${req.shuffleId}, ${req.numPartitions} partitions (${workerList.size} workers, ${state.excludedWorkers.size} excluded)")
            Effect.persist(event).thenRun { _ =>
              replyTo ! RegisterShuffleResponse(locations)
            }
          }
        }

      // ==================== Revive（故障转移） ====================
      case HandleRevive(req, replyTo) =>
        Metrics.MasterMetrics.reviveRequests.inc()
        val aliveWorkers = state.workers.filter { case (_, w) => w.isAlive && !state.excludedWorkers.contains(w.workerId) }.values.toSeq
        if (aliveWorkers.isEmpty) {
          val fallback = PartitionLocation(
            PartitionId(req.shuffleId, req.partitionIndex), 0,
            WorkerAddress("dead", 0, 0), None
          )
          replyTo ! ReviveResponse(fallback)
          Effect.none
        } else {
          state.shuffles.get(req.shuffleId).flatMap(_.partitions.get(req.partitionIndex)) match {
            case Some(original) =>
              val candidates = aliveWorkers.filterNot(_.address == original.primary)
              val chosen = if (candidates.nonEmpty) candidates else aliveWorkers
              val w = chosen(req.partitionIndex % chosen.size)

              // 如果原来的位置有 replica，新 primary 可以从中恢复
              val replicaToRecover = original.replica

              val newLoc = PartitionLocation(
                id = PartitionId(req.shuffleId, req.partitionIndex),
                epoch = original.epoch + 1,
                primary = w.address,
                replica = None  // 新 primary 暂不设 replica
              )

              val event = ShuffleRevived(req.shuffleId, req.partitionIndex,
                original.epoch, newLoc.epoch, newLoc)
              ctx.log.info(s"Revive partition: ${req.shuffleId}[${req.partitionIndex}] " +
                s"epoch ${original.epoch}→${newLoc.epoch}, " +
                s"worker ${original.primary.host}→${w.address.host}" +
                replicaToRecover.map(r => s", replica=${r.host}:${r.port}").getOrElse(""))

              Effect.persist(event).thenRun { _ =>
                replyTo ! ReviveResponse(newLoc, replicaToRecover)
              }

            case None =>
              val fallback = PartitionLocation(
                PartitionId(req.shuffleId, req.partitionIndex), 0,
                WorkerAddress("unknown", 0, 0), None
              )
              replyTo ! ReviveResponse(fallback)
              Effect.none
          }
        }

      // ==================== 查询 Shuffle 位置 ====================
      case HandleGetShuffleLocations(req, replyTo) =>
        val locs = state.shuffles.get(req.shuffleId).map(_.partitions.values.toSeq).getOrElse(Seq.empty)
        replyTo ! GetShuffleLocationsResponse(locs)
        Effect.none

      // ==================== 释放 Slot ====================
      case HandleReleaseSlots(req) =>
        ctx.log.info(s"Release slots for shuffle ${req.shuffleId}: workers ${req.workerIds.mkString(",")}")
        Effect.none

      // ==================== 注销 Shuffle ====================
      case HandleUnregisterShuffle(req) =>
        val event = ShuffleUnregistered(req.shuffleId)
        Metrics.MasterMetrics.shufflesActive.dec()
        ctx.log.info(s"Shuffle unregistered: ${req.shuffleId}")
        Effect.persist(event)

      // ==================== 分区拆分 ====================
      case HandleReportPartitionSplit(req) =>
        val event = PartitionSplit(req.shuffleId, req.partitionIndex, req.subPartitions)
        ctx.log.info(s"Partition split: ${req.shuffleId}[${req.partitionIndex}] → ${req.subPartitions} sub-partitions (threshold=${req.splitThreshold})")
        Effect.persist(event)

      // ==================== 分区提交 ====================
      case HandlePartitionCommitted(req) =>
        val event = PartitionCommitRecorded(req.shuffleId, req.partitionIndex, req.workerId, req.dataSize)
        ctx.log.info(s"Partition committed: ${req.shuffleId}[${req.partitionIndex}], worker=${req.workerId}, size=${req.dataSize}")
        Effect.persist(event)

      // ==================== 查询分区是否就绪 ====================
      case HandleIsPartitionReady(req, replyTo) =>
        // 两阶段提交：只有当 shuffle 完全 committed（所有 mapper 完成）才返回 true
        val ready = state.shuffleCommitted.contains(req.shuffleId)
        replyTo ! IsPartitionReadyResponse(ready)
        Effect.none

      // ==================== 报告 Worker 失败 ====================
      case HandleReportWorkerFailure(req) =>
        val current = state.workerFailureCounts.getOrElse(req.workerId, 0) + 1
        val maxFailures = 5
        if (current >= maxFailures) {
          ctx.log.warn(s"Worker ${req.workerId} excluded after $current consecutive failures. Last: ${req.errorMessage}")
          Effect.persist(WorkerExcluded(req.workerId, req.errorMessage))
        } else {
          ctx.log.warn(s"Worker ${req.workerId} failure #$current/$maxFailures: ${req.errorMessage}")
          Effect.none
        }

      // ==================== MapperEnd（两阶段提交阶段一） ====================
      case HandleMapperEnd(req, replyTo) =>
        val sid = req.shuffleId
        val event = MapperCompleted(sid, req.mapperId)
        val alreadyDone = state.mapperCompletions.getOrElse(sid, Set.empty)
        val newDone = alreadyDone + req.mapperId
        val totalMappers = state.shuffleNumMappers.getOrElse(sid, 1)

        ctx.log.info(s"MapperEnd: $sid mapper=${req.mapperId}, progress=${newDone.size}/$totalMappers")

        if (newDone.size >= totalMappers) {
          // 所有 mapper 完成 → 触发 CommitShuffle
          ctx.log.info(s"All $totalMappers mappers completed for $sid, broadcasting CommitShuffle to workers")

          // 找到该 shuffle 涉及的所有 worker
          state.shuffles.get(sid).foreach { info =>
            val workerSet = info.partitions.values.flatMap { loc =>
              Seq(loc.primary) ++ loc.replica.toSeq
            }.toSet

            workerSet.foreach { addr =>
              val workerPath = s"pekko://meteor-worker@${addr.host}:${addr.rpcPort}/user/worker"
              ctx.log.info(s"Sending CommitShuffle to worker at $workerPath")
              // 通过 actorSelection 发送 CommitShuffle
              import org.apache.pekko.actor.typed.scaladsl.adapter._
              val classicSystem = ctx.system.toClassic
              import scala.concurrent.ExecutionContext.Implicits.global
              classicSystem.actorSelection(workerPath).resolveOne(5.seconds).foreach { ref =>
                ref ! CommitShuffle(sid)
              }
            }
          }

          // ShuffleCommitted 逻辑内嵌到 MapperCompleted 的 event handler 中
          Effect.persist(event).thenRun { _ =>
            replyTo ! MapperEndResponse(sid, acknowledged = true)
          }
        } else {
          Effect.persist(event).thenRun { _ =>
            replyTo ! MapperEndResponse(sid, acknowledged = true)
          }
        }

      // ==================== CommitShuffle 响应 ====================
      case HandleCommitShuffleResponse(req) =>
        if (req.success) {
          ctx.log.info(s"CommitShuffle acknowledged for ${req.shuffleId}")
        } else {
          ctx.log.warn(s"CommitShuffle failed for ${req.shuffleId}")
        }
        Effect.none

      // ==================== 状态查询 ====================
      case QueryWorkers(replyTo) =>
        val infos = state.workers.values.map { w =>
          WorkerStateInfo(w.workerId, w.address.host, w.address.port,
            w.diskSlotsFree, w.memorySlotsFree, w.lastHeartbeatMs, w.isAlive)
        }.toSeq
        replyTo ! WorkersQueryResponse(infos)
        Effect.none

      case QueryShuffles(replyTo) =>
        val infos = state.shuffles.values.map { s =>
          ShuffleStateInfo(s.shuffleId.appId, s.shuffleId.shuffleNum,
            s.partitions.size, s.subPartitionCounts)
        }.toSeq
        replyTo ! ShufflesQueryResponse(infos)
        Effect.none

      case GracefulShutdown =>
        ctx.log.info(s"Master shutting down gracefully. ${state.workers.size} workers, ${state.shuffles.size} shuffles.")
        Metrics.MasterMetrics.workersAlive.set(0)
        Metrics.MasterMetrics.shufflesActive.set(0)
        Effect.persist(ShuttingDown).thenRun { _ =>
          ctx.log.info("Master shutdown complete.")
        }

      // ==================== 定时清理过期 Shuffle ====================
      case CheckShuffleCleanup =>
        val staleTimeoutMs = 600000L  // 10 分钟（可配置）
        val now = System.currentTimeMillis()
        val staleShuffles = state.shuffleRegisterTimestamps.filter {
          case (_, ts) => now - ts > staleTimeoutMs
        }.keys.toSeq

        if (staleShuffles.nonEmpty) {
          ctx.log.info(s"Cleaning up ${staleShuffles.size} stale shuffles (registered > ${staleTimeoutMs / 1000}s ago)")
          staleShuffles.foreach { sid =>
            // 通知相关 Workers 释放数据
            state.shuffles.get(sid).foreach { info =>
              info.partitions.values.map(_.primary).toSeq.distinct.foreach { addr =>
                val workerId = s"${addr.host}:${addr.port}"
                ctx.log.info(s"Requesting worker $workerId to release shuffle $sid")
              }
            }
            // 注销 shuffle
            ctx.self ! HandleUnregisterShuffle(UnregisterShuffle(sid))
          }
        }
        ctx.scheduleOnce(300.seconds, ctx.self, CheckShuffleCleanup)
        Effect.none
    }
  }

  // ================================
  // 无持久化模式（测试用）
  // ================================

  private def handleCommand(ctx: ActorContext[Command])
    : (MasterState, Command) => MasterState = { (state, cmd) =>

    cmd match {
      case WrappedClusterEvent(_) => state

      case HandleRegisterWorker(req, replyTo) =>
        val workerId = s"${req.address.host}:${req.address.port}"
        state.workers.get(workerId) match {
          case Some(existing) if existing.isAlive =>
            replyTo ! RegisterWorkerResponse(success = true, workerId)
            state
          case _ =>
            val event = WorkerRegistered(workerId, req.address, req.diskSlots, req.memorySlots)
            replyTo ! RegisterWorkerResponse(success = true, workerId)
            applyEvent(state, event)
        }

      case HandleHeartbeat(req) =>
        val event = WorkerHeartbeat(req.workerId, req.diskSlotsFree, req.memorySlotsFree, System.currentTimeMillis())
        applyEvent(state, event)

      case HandleUnregisterWorker(req) =>
        val event = WorkerTimedOut(req.workerId, System.currentTimeMillis())
        applyEvent(state, event)

      case WorkerTimeout(workerId) =>
        state.workers.get(workerId) match {
          case Some(w) if !w.isAlive =>
            applyEvent(state, WorkerTimedOut(workerId, System.currentTimeMillis()))
          case _ => state
        }

      case CheckWorkerHealth =>
        val dead = state.workers.filter { case (_, w) => !w.isAlive }
        dead.keys.foreach { id => ctx.self ! WorkerTimeout(id) }
        ctx.scheduleOnce(30.seconds, ctx.self, CheckWorkerHealth)
        state

      case HandleRegisterShuffle(req, replyTo) =>
        if (state.shuttingDown) {
          replyTo ! RegisterShuffleResponse(Seq.empty)
          state
        } else {
          val aliveWorkers = state.workers.filter { case (_, w) => w.isAlive && !state.excludedWorkers.contains(w.workerId) }
          if (aliveWorkers.isEmpty) {
            replyTo ! RegisterShuffleResponse(Seq.empty)
            state
          } else {
            val workerList = aliveWorkers.values.toSeq
            val locations = (0 until req.numPartitions).map { i =>
              val w = workerList(i % workerList.size)
              PartitionLocation(PartitionId(req.shuffleId, i), 0, w.address, None)
            }
            replyTo ! RegisterShuffleResponse(locations)
            applyEvent(state, ShuffleRegistered(req.shuffleId, req.numPartitions, locations, req.numMappers))
          }
        }

      case HandleRevive(req, replyTo) =>
        val aliveWorkers = state.workers.filter { case (_, w) => w.isAlive && !state.excludedWorkers.contains(w.workerId) }.values.toSeq
        if (aliveWorkers.isEmpty) {
          replyTo ! ReviveResponse(PartitionLocation(PartitionId(req.shuffleId, req.partitionIndex), 0,
            WorkerAddress("dead", 0, 0), None))
          state
        } else {
          state.shuffles.get(req.shuffleId).flatMap(_.partitions.get(req.partitionIndex)) match {
            case Some(original) =>
              val candidates = aliveWorkers.filterNot(_.address == original.primary)
              val chosen = if (candidates.nonEmpty) candidates else aliveWorkers
              val w = chosen(req.partitionIndex % chosen.size)
              val newLoc = PartitionLocation(PartitionId(req.shuffleId, req.partitionIndex),
                original.epoch + 1, w.address, None)
              replyTo ! ReviveResponse(newLoc, original.replica)
              applyEvent(state, ShuffleRevived(req.shuffleId, req.partitionIndex,
                original.epoch, newLoc.epoch, newLoc))
            case None =>
              replyTo ! ReviveResponse(PartitionLocation(PartitionId(req.shuffleId, req.partitionIndex), 0,
                WorkerAddress("unknown", 0, 0), None))
              state
          }
        }

      case HandleGetShuffleLocations(req, replyTo) =>
        val locs = state.shuffles.get(req.shuffleId).map(_.partitions.values.toSeq).getOrElse(Seq.empty)
        replyTo ! GetShuffleLocationsResponse(locs)
        state

      case HandleReleaseSlots(req) =>
        state

      case HandleUnregisterShuffle(req) =>
        applyEvent(state, ShuffleUnregistered(req.shuffleId))

      case HandleReportPartitionSplit(req) =>
        applyEvent(state, PartitionSplit(req.shuffleId, req.partitionIndex, req.subPartitions))

      case HandlePartitionCommitted(req) =>
        applyEvent(state, PartitionCommitRecorded(req.shuffleId, req.partitionIndex, req.workerId, req.dataSize))

      case HandleReportWorkerFailure(req) =>
        val current = state.workerFailureCounts.getOrElse(req.workerId, 0) + 1
        if (current >= 5) {
          applyEvent(state, WorkerExcluded(req.workerId, req.errorMessage))
        } else {
          state.copy(workerFailureCounts = state.workerFailureCounts + (req.workerId -> current))
        }

      // ==================== MapperEnd（两阶段提交阶段一） ====================
      case HandleMapperEnd(req, replyTo) =>
        val sid = req.shuffleId
        val alreadyDone = state.mapperCompletions.getOrElse(sid, Set.empty)
        val newDone = alreadyDone + req.mapperId
        val totalMappers = state.shuffleNumMappers.getOrElse(sid, 1)

        ctx.log.info(s"[NonPersistent] MapperEnd: $sid mapper=${req.mapperId}, progress=${newDone.size}/$totalMappers")

        val s1 = applyEvent(state, MapperCompleted(sid, req.mapperId))

        if (newDone.size >= totalMappers) {
          ctx.log.info(s"[NonPersistent] All $totalMappers mappers completed for $sid, committing")
          state.shuffles.get(sid).foreach { info =>
            val workerSet = info.partitions.values.flatMap { loc =>
              Seq(loc.primary) ++ loc.replica.toSeq
            }.toSet
            workerSet.foreach { addr =>
              val workerPath = s"pekko://meteor-worker@${addr.host}:${addr.rpcPort}/user/worker"
              import org.apache.pekko.actor.typed.scaladsl.adapter._
              val classicSystem = ctx.system.toClassic
              import scala.concurrent.ExecutionContext.Implicits.global
              classicSystem.actorSelection(workerPath).resolveOne(5.seconds).foreach { ref =>
                ref ! CommitShuffle(sid)
              }
            }
          }
          val s2 = applyEvent(s1, ShuffleCommitted(sid))
          replyTo ! MapperEndResponse(sid, acknowledged = true)
          s2
        } else {
          replyTo ! MapperEndResponse(sid, acknowledged = true)
          s1
        }

      case HandleCommitShuffleResponse(req) =>
        ctx.log.info(s"[NonPersistent] CommitShuffle ack for ${req.shuffleId}: ${req.success}")
        state

      // ==================== 查询分区是否就绪（两阶段提交版本） ====================
      case HandleIsPartitionReady(req, replyTo) =>
        val ready = state.shuffleCommitted.contains(req.shuffleId)
        replyTo ! IsPartitionReadyResponse(ready)
        state

      // ==================== 状态查询 ====================
      case QueryWorkers(replyTo) =>
        val infos = state.workers.values.map { w =>
          WorkerStateInfo(w.workerId, w.address.host, w.address.port,
            w.diskSlotsFree, w.memorySlotsFree, w.lastHeartbeatMs, w.isAlive)
        }.toSeq
        replyTo ! WorkersQueryResponse(infos)
        state

      case QueryShuffles(replyTo) =>
        val infos = state.shuffles.values.map { s =>
          ShuffleStateInfo(s.shuffleId.appId, s.shuffleId.shuffleNum,
            s.partitions.size, s.subPartitionCounts)
        }.toSeq
        replyTo ! ShufflesQueryResponse(infos)
        state

      case GracefulShutdown =>
        ctx.log.info(s"Master shutting down gracefully. ${state.workers.size} workers, ${state.shuffles.size} shuffles.")
        state.copy(shuttingDown = true)

      case CheckShuffleCleanup =>
        val staleTimeoutMs = 600000L
        val now = System.currentTimeMillis()
        val staleShuffles = state.shuffleRegisterTimestamps.filter {
          case (_, ts) => now - ts > staleTimeoutMs
        }.keys.toSeq
        if (staleShuffles.nonEmpty) {
          ctx.log.info(s"[NonPersistent] Cleaning up ${staleShuffles.size} stale shuffles")
          staleShuffles.foreach { sid =>
            ctx.self ! HandleUnregisterShuffle(UnregisterShuffle(sid))
          }
        }
        ctx.scheduleOnce(300.seconds, ctx.self, CheckShuffleCleanup)
        state
    }
  }
}

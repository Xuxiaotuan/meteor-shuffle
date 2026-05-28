package cn.xuyinyin.meteor.client

import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.util.Timeout
import scala.concurrent.duration._

import cn.xuyinyin.meteor.common.{Protocol => Proto, MasterActorCommand}
import Proto._

/**
 * LifecycleManager — JM 侧 Shuffle 生命周期管理器
 *
 * 对标 Celeborn 的 LifecycleManager：
 *   1. RegisterShuffle → 向 Master 请求分区 slot，缓存 PartitionLocation
 *   2. 跟踪 Mapper 提交状态（partition → committed flag）
 *   3. CommitPartitions → 所有 mapper 完成后，标记分区可读
 *   4. UnregisterShuffle → 通知 Master 释放资源
 *
 * JM 侧角色：
 *   - 负责与 Master 通信（注册、注销、Revive）
 *   - 管理 shuffle 级状态（locations、commit 进度）
 *   - TM 侧的 ShuffleClient 专注于数据推送/拉取
 */
object LifecycleManager {

  sealed trait Command

  // ===== JM 调用 =====
  /** 注册一个 shuffle，返回分区位置 */
  final case class RegisterShuffle(
    shuffleId: ShuffleId,
    numPartitions: Int,
    replyTo: ActorRef[RegisterShuffleResponse]
  ) extends Command

  /** Mapper 完成后提交一个分区 */
  final case class CommitPartition(
    shuffleId: ShuffleId,
    partitionIndex: Int,
    attemptNumber: Int,
    replyTo: ActorRef[CommitPartitionResponse]
  ) extends Command

  /** 检查某个分区的所有 mapper 是否全部提交 */
  final case class WaitForCommit(
    shuffleId: ShuffleId,
    partitionIndex: Int,
    replyTo: ActorRef[CommitStatusResponse]
  ) extends Command

  /** 注销 shuffle（释放 Master 和 Worker 上的资源） */
  final case class UnregisterShuffle(
    shuffleId: ShuffleId,
    replyTo: ActorRef[UnregisterShuffleResponse]
  ) extends Command

  // ===== 查询 =====
  final case class GetShuffleState(
    shuffleId: ShuffleId,
    replyTo: ActorRef[ShuffleStateResponse]
  ) extends Command

  // ===== 响应类型 =====
  final case class CommitPartitionResponse(success: Boolean, shuffleId: ShuffleId, partitionIndex: Int)
  final case class CommitStatusResponse(
    shuffleId: ShuffleId,
    partitionIndex: Int,
    committedMappers: Int,
    totalMappers: Int,
    allCommitted: Boolean
  )
  final case class UnregisterShuffleResponse(success: Boolean, shuffleId: ShuffleId)
  final case class ShuffleStateResponse(
    shuffleId: ShuffleId,
    numPartitions: Int,
    locations: Seq[PartitionLocation],
    committedPartitions: Set[Int],
    numCommittedPartitions: Int
  )

  // ===== 内部 =====
  private final case class MasterRegisterReply(
    shuffleId: ShuffleId,
    response: RegisterShuffleResponse,
    replyTo: ActorRef[RegisterShuffleResponse]
  ) extends Command

  private final case class MasterUnregisterReply(
    shuffleId: ShuffleId,
    replyTo: ActorRef[UnregisterShuffleResponse]
  ) extends Command

  private final case class MasterReviveReply(
    shuffleId: ShuffleId,
    partitionIndex: Int,
    response: ReviveResponse
  ) extends Command

  // ================================
  // 状态
  // ================================

  /** 单个 shuffle 的状态 */
  case class ShuffleState(
    shuffleId: ShuffleId,
    numPartitions: Int,
    locations: Map[Int, PartitionLocation],                 // partitionIndex → location
    committedMappers: Map[Int, Int] = Map.empty,            // partitionIndex → committed mapper count
    totalMappersPerPartition: Map[Int, Int] = Map.empty,    // partitionIndex → total mapper count
    committedPartitions: Set[Int] = Set.empty               // fully committed partition indices
  )

  // ================================
  // 工厂
  // ================================

  def apply(
    masterRef: ActorRef[MasterActorCommand],
    masterAskTimeout: FiniteDuration = 10.seconds
  ): Behavior[Command] = Behaviors.setup { ctx =>
    ctx.log.info("LifecycleManager starting")
    active(ctx, masterRef, masterAskTimeout, Map.empty)
  }

  // ================================
  // 活跃状态
  // ================================

  private def active(
    ctx: ActorContext[Command],
    masterRef: ActorRef[MasterActorCommand],
    askTimeout: FiniteDuration,
    shuffleStates: Map[ShuffleId, ShuffleState]
  ): Behavior[Command] = {

    implicit val timeout: Timeout = Timeout(askTimeout)
    implicit val scheduler = ctx.system.scheduler
    import org.apache.pekko.actor.typed.scaladsl.AskPattern._

    Behaviors.receiveMessage {

      // ==================== 注册 Shuffle ====================
      case RegisterShuffle(shuffleId, numPartitions, replyTo) =>
        ctx.log.info(s"Registering shuffle $shuffleId, $numPartitions partitions")

        val req = Proto.RegisterShuffle(shuffleId, numPartitions)
        ctx.pipeToSelf(
          masterRef.ask[RegisterShuffleResponse](replyTo =>
            MasterActorCommand.WrappedRegisterShuffle(req, replyTo))
        ) {
          case scala.util.Success(resp) =>
            MasterRegisterReply(shuffleId, resp, replyTo)
          case scala.util.Failure(ex) =>
            ctx.log.error(s"Register shuffle failed: ${ex.getMessage}")
            MasterRegisterReply(shuffleId, RegisterShuffleResponse(Seq.empty), replyTo)
        }

        Behaviors.same

      case MasterRegisterReply(shuffleId, resp, replyTo) =>
        val locations = resp.locations.map(l => l.id.partitionIndex -> l).toMap
        val state = ShuffleState(
          shuffleId = shuffleId,
          numPartitions = resp.locations.size,
          locations = locations
        )
        ctx.log.info(s"Shuffle $shuffleId registered, ${resp.locations.size} partitions")
        replyTo ! resp
        active(ctx, masterRef, askTimeout, shuffleStates + (shuffleId -> state))

      // ==================== 提交分区 ====================
      case CommitPartition(shuffleId, partitionIndex, attemptNumber, replyTo) =>
        shuffleStates.get(shuffleId) match {
          case Some(state) =>
            val current = state.committedMappers.getOrElse(partitionIndex, 0)
            val total = state.totalMappersPerPartition.getOrElse(partitionIndex, 1) // 默认1个mapper
            val newCount = current + 1
            val allCommitted = newCount >= total

            val newCommittedParts = if (allCommitted) {
              ctx.log.info(s"Partition $shuffleId:$partitionIndex fully committed ($newCount/$total mappers)")
              state.committedPartitions + partitionIndex
            } else {
              ctx.log.info(s"Partition $shuffleId:$partitionIndex mapper $newCount/$total committed")
              state.committedPartitions
            }

            val newMappers = state.committedMappers + (partitionIndex -> newCount)
            val newState = state.copy(
              committedMappers = newMappers,
              committedPartitions = newCommittedParts
            )

            replyTo ! CommitPartitionResponse(success = true, shuffleId, partitionIndex)
            active(ctx, masterRef, askTimeout, shuffleStates + (shuffleId -> newState))

          case None =>
            ctx.log.warn(s"Commit for unknown shuffle $shuffleId")
            replyTo ! CommitPartitionResponse(success = false, shuffleId, partitionIndex)
            Behaviors.same
        }

      // ==================== 等待提交完成 ====================
      case WaitForCommit(shuffleId, partitionIndex, replyTo) =>
        shuffleStates.get(shuffleId) match {
          case Some(state) =>
            val committed = state.committedMappers.getOrElse(partitionIndex, 0)
            val total = state.totalMappersPerPartition.getOrElse(partitionIndex, 1)
            replyTo ! CommitStatusResponse(
              shuffleId, partitionIndex,
              committedMappers = committed,
              totalMappers = total,
              allCommitted = state.committedPartitions.contains(partitionIndex)
            )
          case None =>
            replyTo ! CommitStatusResponse(shuffleId, partitionIndex, 0, 0, allCommitted = false)
        }
        Behaviors.same

      // ==================== 注销 Shuffle ====================
      case UnregisterShuffle(shuffleId, replyTo) =>
        ctx.log.info(s"Unregistering shuffle $shuffleId")

        val unregisterReq = Proto.UnregisterShuffle(shuffleId)
        // Fire-and-forget to Master
        masterRef ! MasterActorCommand.WrappedUnregisterShuffle(unregisterReq)

        // Notify workers to release slots (via Master)
        shuffleStates.get(shuffleId).foreach { state =>
          val workerIds = state.locations.values.map(loc => s"${loc.primary.host}:${loc.primary.port}").toSet
          if (workerIds.nonEmpty) {
            val releaseMsg = ReleaseSlots(shuffleId, workerIds)
            masterRef ! MasterActorCommand.WrappedReleaseSlots(releaseMsg)
          }
        }

        replyTo ! UnregisterShuffleResponse(success = true, shuffleId)
        active(ctx, masterRef, askTimeout, shuffleStates - shuffleId)

      // ==================== 查询 ====================
      case GetShuffleState(shuffleId, replyTo) =>
        shuffleStates.get(shuffleId) match {
          case Some(state) =>
            replyTo ! ShuffleStateResponse(
              shuffleId, state.numPartitions,
              state.locations.values.toSeq,
              state.committedPartitions,
              state.committedPartitions.size
            )
          case None =>
            replyTo ! ShuffleStateResponse(shuffleId, 0, Seq.empty, Set.empty, 0)
        }
        Behaviors.same
    }
  }
}

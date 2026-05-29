package cn.xuyinyin.meteor.common

import org.apache.pekko.actor.typed.receptionist.ServiceKey
import cn.xuyinyin.meteor.common.Protocol._

/**
 * Pekko Receptionist 服务注册 Key
 */
object ServiceKeys {

  /** Master 服务 Key — Worker / Client 通过 Receptionist 发现 Master */
  val MasterService: ServiceKey[MasterActorCommand] =
    ServiceKey[MasterActorCommand]("meteor-master")

  /** Worker 服务 Key（未来可用于健康检查聚合） */
  val WorkerService: ServiceKey[WorkerActorCommand] =
    ServiceKey[WorkerActorCommand]("meteor-worker")
}

/**
 * Master Actor 可接收的消息（子集，用于 Receptionist 注册）
 *
 * 需要响应的命令携带 replyTo: ActorRef[Any]，
 * Master 处理完后通过 replyTo 回复对应类型的响应。
 */
sealed trait MasterActorCommand extends java.io.Serializable

object MasterActorCommand {
  // 需要响应
  final case class WrappedRegisterWorker(worker: RegisterWorker, replyTo: org.apache.pekko.actor.typed.ActorRef[RegisterWorkerResponse]) extends MasterActorCommand
  final case class WrappedRegisterShuffle(rs: RegisterShuffle, replyTo: org.apache.pekko.actor.typed.ActorRef[RegisterShuffleResponse]) extends MasterActorCommand
  final case class WrappedRevive(r: Revive, replyTo: org.apache.pekko.actor.typed.ActorRef[ReviveResponse]) extends MasterActorCommand
  final case class WrappedGetLocations(gl: GetShuffleLocations, replyTo: org.apache.pekko.actor.typed.ActorRef[GetShuffleLocationsResponse]) extends MasterActorCommand
  // Fire-and-forget
  final case class WrappedHeartbeat(hb: Heartbeat) extends MasterActorCommand
  final case class WrappedUnregisterWorker(uw: UnregisterWorker) extends MasterActorCommand
  final case class WrappedReleaseSlots(rs: ReleaseSlots) extends MasterActorCommand
  final case class WrappedUnregisterShuffle(us: UnregisterShuffle) extends MasterActorCommand
  final case class WrappedReportPartitionSplit(rps: ReportPartitionSplit) extends MasterActorCommand
  final case class WrappedPartitionCommitted(pc: PartitionCommitted) extends MasterActorCommand
  final case class WrappedIsPartitionReady(ipr: IsPartitionReady, replyTo: org.apache.pekko.actor.typed.ActorRef[IsPartitionReadyResponse]) extends MasterActorCommand
  final case class WrappedReportWorkerFailure(rwf: ReportWorkerFailure) extends MasterActorCommand
  final case class WrappedMapperEnd(me: MapperEnd, replyTo: org.apache.pekko.actor.typed.ActorRef[MapperEndResponse]) extends MasterActorCommand
  final case class WrappedCommitShuffleResponse(csr: CommitShuffleResponse) extends MasterActorCommand
}

/**
 * Worker Actor 可接收的消息（子集，用于 Receptionist 注册）
 */
sealed trait WorkerActorCommand

object WorkerActorCommand {
  final case class WrappedPushData(pd: PushData) extends WorkerActorCommand
  final case class WrappedFetchData(fd: FetchData) extends WorkerActorCommand
  final case class WrappedReplicateData(rd: ReplicateData) extends WorkerActorCommand
  final case class WrappedReleaseSlots(rs: ReleaseSlots) extends WorkerActorCommand
  /** Master → Worker: 从副本恢复数据 */
  final case class WrappedRecoverFromReplica(rfr: RecoverFromReplica) extends WorkerActorCommand
  /** Master → Worker: 两阶段提交——所有 mapper 完成，可以提交 */
  final case class WrappedCommitShuffle(cs: CommitShuffle, replyTo: org.apache.pekko.actor.typed.ActorRef[CommitShuffleResponse]) extends WorkerActorCommand
}

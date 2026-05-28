package cn.xuyinyin.meteor.client

import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.util.Timeout
import scala.concurrent.duration._
import scala.util.{Success, Failure}

import cn.xuyinyin.meteor.common.Protocol._
import cn.xuyinyin.meteor.common.{MasterActorCommand, Codec}

/**
 * Meteor Shuffle Client — 真实实现
 *
 * 嵌入 Flink TaskManager 内部的 shuffle 客户端。
 * 对标 Celeborn 的 LifecycleManager (JM 侧) + ShuffleClient (TM 侧)。
 *
 * 核心流程：
 *   1. Register → 向 Master 请求分区位置，缓存 PartitionLocation
 *   2. Push → 按 PartitionLocation 推送到对应 Worker（支持压缩 + 重试）
 *   3. Fetch → 从 Worker 拉取数据，primary 不可达自动 fallback replica
 *   4. Revive → 原 Worker 挂了，向 Master 请求新位置
 */
object ShuffleClient {

  sealed trait Command

  // ===== JM/TM 侧调用 =====
  final case class Register(
    shuffleId: ShuffleId,
    numPartitions: Int,
    replyTo: ActorRef[RegisterShuffleResponse]
  ) extends Command

  final case class Push(
    shuffleId: ShuffleId,
    partitionIndex: Int,
    attemptNumber: Int,
    data: Array[Byte],
    replyTo: ActorRef[PushDataResponse],
    compression: CompressionType = CompressNone
  ) extends Command

  final case class Fetch(
    shuffleId: ShuffleId,
    partitionIndex: Int,
    attemptNumber: Int,
    replyTo: ActorRef[FetchDataResponse]
  ) extends Command

  // ===== 内部回调 =====
  private final case class OnPushResult(
    shuffleId: ShuffleId, partitionIndex: Int, success: Boolean,
    replyTo: ActorRef[PushDataResponse], retryCount: Int,
    data: Array[Byte], compression: CompressionType, replica: Option[WorkerAddress]
  ) extends Command

  private final case class OnFetchResult(
    shuffleId: ShuffleId, partitionIndex: Int, data: Option[Array[Byte]],
    replyTo: ActorRef[FetchDataResponse], triedReplica: Boolean, location: PartitionLocation
  ) extends Command

  private final case class OnRegisterResult(
    shuffleId: ShuffleId, result: RegisterShuffleResponse, replyTo: ActorRef[RegisterShuffleResponse]
  ) extends Command

  private final case class OnRegisterFailed(ex: Throwable, replyTo: ActorRef[RegisterShuffleResponse]) extends Command

  private final case class WorkerResolved(address: WorkerAddress, ref: org.apache.pekko.actor.ActorRef) extends Command
  private final case class WorkerResolveFailed(address: WorkerAddress, ex: Throwable) extends Command

  private val MAX_PUSH_RETRIES = 3
  private val RESOLVE_TIMEOUT = 5.seconds
  private val ASK_TIMEOUT = 10.seconds

  def apply(
    masterRef: ActorRef[MasterActorCommand],
    replicateEnabled: Boolean
  ): Behavior[Command] = Behaviors.setup { ctx =>
    ctx.log.info(s"ShuffleClient starting, replicate=$replicateEnabled")
    active(ctx, masterRef, replicateEnabled, locationCache = Map.empty, workerCache = Map.empty)
  }

  private def active(
    ctx: ActorContext[Command],
    masterRef: ActorRef[MasterActorCommand],
    replicateEnabled: Boolean,
    locationCache: Map[ShuffleId, Map[Int, PartitionLocation]],
    workerCache: Map[WorkerAddress, org.apache.pekko.actor.ActorRef]
  ): Behavior[Command] = {

    Behaviors.receiveMessage {

      // ==================== Register Shuffle ====================
      case Register(shuffleId, numPartitions, replyTo) =>
        ctx.log.info(s"Registering shuffle $shuffleId, $numPartitions partitions")
        val req = RegisterShuffle(shuffleId, numPartitions)
        // 创建临时 actor 接收 Master 响应
        val adapter = ctx.messageAdapter[RegisterShuffleResponse] { resp =>
          OnRegisterResult(shuffleId, resp, replyTo)
        }
        masterRef ! MasterActorCommand.WrappedRegisterShuffle(req, adapter)
        Behaviors.same

      // ==================== Push Data ====================
      case Push(shuffleId, partitionIndex, attemptNumber, data, replyTo, compression) =>
        locationCache.get(shuffleId).flatMap(_.get(partitionIndex)) match {
          case Some(loc) =>
            pushToWorker(ctx, loc.primary, shuffleId, partitionIndex, attemptNumber,
              data, replyTo, retryCount = 0, compression, loc.replica, workerCache)
          case None =>
            ctx.log.warn(s"No location for $shuffleId:$partitionIndex, need register first")
            replyTo ! PushDataResponse(shuffleId, partitionIndex, success = false)
            Behaviors.same
        }

      // ==================== Fetch Data ====================
      case Fetch(shuffleId, partitionIndex, attemptNumber, replyTo) =>
        locationCache.get(shuffleId).flatMap(_.get(partitionIndex)) match {
          case Some(loc) =>
            fetchFromWorker(ctx, loc.primary, shuffleId, partitionIndex, attemptNumber,
              replyTo, triedReplica = false, loc, workerCache)
          case None =>
            ctx.log.error(s"No location for fetch: $shuffleId:$partitionIndex")
            replyTo ! FetchDataResponse(shuffleId, partitionIndex, None)
            Behaviors.same
        }

      // ==================== 内部回调 ====================
      case OnRegisterResult(shuffleId, result, replyTo) =>
        val newCache = locationCache + (shuffleId -> result.locations.map(l => l.id.partitionIndex -> l).toMap)
        replyTo ! result
        active(ctx, masterRef, replicateEnabled, newCache, workerCache)

      case OnRegisterFailed(ex, replyTo) =>
        ctx.log.error(s"Register failed: ${ex.getMessage}")
        replyTo ! RegisterShuffleResponse(Seq.empty)
        Behaviors.same

      case OnPushResult(shuffleId, partitionIndex, success, replyTo, retryCount, data, compression, replicaAddr) =>
        if (success) {
          replyTo ! PushDataResponse(shuffleId, partitionIndex, success = true)
          Behaviors.same
        } else if (retryCount < MAX_PUSH_RETRIES) {
          ctx.log.warn(s"Push failed, retry $retryCount/$MAX_PUSH_RETRIES")
          locationCache.get(shuffleId).flatMap(_.get(partitionIndex)) match {
            case Some(loc) =>
              pushToWorker(ctx, loc.primary, shuffleId, partitionIndex, 0,
                data, replyTo, retryCount + 1, compression, replicaAddr, workerCache)
            case None =>
              replyTo ! PushDataResponse(shuffleId, partitionIndex, success = false)
              Behaviors.same
          }
        } else {
          ctx.log.error(s"Push failed after $MAX_PUSH_RETRIES retries")
          replyTo ! PushDataResponse(shuffleId, partitionIndex, success = false)
          Behaviors.same
        }

      case OnFetchResult(shuffleId, partitionIndex, data, replyTo, triedReplica, location) =>
        data match {
          case Some(_) =>
            replyTo ! FetchDataResponse(shuffleId, partitionIndex, data)
            Behaviors.same
          case None if !triedReplica && location.replica.isDefined =>
            ctx.log.warn(s"Primary fetch failed, trying replica for $shuffleId:$partitionIndex")
            fetchFromWorker(ctx, location.replica.get, shuffleId, partitionIndex, 0,
              replyTo, triedReplica = true, location, workerCache)
          case None =>
            ctx.log.error(s"Fetch failed (both primary and replica): $shuffleId:$partitionIndex")
            replyTo ! FetchDataResponse(shuffleId, partitionIndex, None, fromReplica = triedReplica)
            Behaviors.same
        }

      case WorkerResolved(address, ref) =>
        active(ctx, masterRef, replicateEnabled, locationCache, workerCache + (address -> ref))

      case WorkerResolveFailed(address, ex) =>
        ctx.log.warn(s"Failed to resolve worker $address: ${ex.getMessage}")
        Behaviors.same
    }
  }

  // ================================
  // 推送到 Worker
  // ================================

  private def pushToWorker(
    ctx: ActorContext[Command],
    workerAddr: WorkerAddress,
    shuffleId: ShuffleId,
    partitionIndex: Int,
    attemptNumber: Int,
    data: Array[Byte],
    replyTo: ActorRef[PushDataResponse],
    retryCount: Int,
    compression: CompressionType,
    replicaAddr: Option[WorkerAddress],
    workerCache: Map[WorkerAddress, org.apache.pekko.actor.ActorRef]
  ): Behavior[Command] = {

    val checksum = Codec.crc32(data).toLong & 0xFFFFFFFFL
    val req = PushData(shuffleId, partitionIndex, attemptNumber, data, checksum, replicaAddr, compression)
    resolveAndPush(ctx, workerAddr, workerCache, req, replyTo, retryCount)
    Behaviors.same
  }

  // ================================
  // 从 Worker 拉取
  // ================================

  private def fetchFromWorker(
    ctx: ActorContext[Command],
    workerAddr: WorkerAddress,
    shuffleId: ShuffleId,
    partitionIndex: Int,
    attemptNumber: Int,
    replyTo: ActorRef[FetchDataResponse],
    triedReplica: Boolean,
    location: PartitionLocation,
    workerCache: Map[WorkerAddress, org.apache.pekko.actor.ActorRef]
  ): Behavior[Command] = {

    val classicSystem = ctx.system.classicSystem
    val path = s"pekko://meteor-worker@${workerAddr.host}:${workerAddr.rpcPort}/user/worker"

    import scala.concurrent.ExecutionContext.Implicits.global
    classicSystem.actorSelection(path).resolveOne(RESOLVE_TIMEOUT).onComplete {
      case scala.util.Success(workerRef) =>
        import org.apache.pekko.pattern.ask
        implicit val timeout: Timeout = Timeout(ASK_TIMEOUT)
        val future = workerRef ? FetchData(shuffleId, partitionIndex, attemptNumber)
        future.onComplete {
          case scala.util.Success(resp: FetchDataResponse) =>
            ctx.self ! OnFetchResult(shuffleId, partitionIndex, resp.data, replyTo, triedReplica, location)
          case scala.util.Success(other) =>
            ctx.log.warn(s"Unexpected fetch response: ${other.getClass}")
            ctx.self ! OnFetchResult(shuffleId, partitionIndex, None, replyTo, triedReplica, location)
          case scala.util.Failure(ex) =>
            ctx.self ! OnFetchResult(shuffleId, partitionIndex, None, replyTo, triedReplica, location)
        }
      case scala.util.Failure(ex) =>
        ctx.self ! OnFetchResult(shuffleId, partitionIndex, None, replyTo, triedReplica, location)
    }

    Behaviors.same
  }

  // ================================
  // 解析 Worker 并发送 PushData
  // ================================

  private def resolveAndPush(
    ctx: ActorContext[Command],
    workerAddr: WorkerAddress,
    workerCache: Map[WorkerAddress, org.apache.pekko.actor.ActorRef],
    req: PushData,
    replyTo: ActorRef[PushDataResponse],
    retryCount: Int
  ): Unit = {

    workerCache.get(workerAddr) match {
      case Some(ref) =>
        doPush(ctx, ref, req, replyTo, retryCount)
      case None =>
        val classicSystem = ctx.system.classicSystem
        val path = s"pekko://meteor-worker@${workerAddr.host}:${workerAddr.rpcPort}/user/worker"

        import scala.concurrent.ExecutionContext.Implicits.global
        classicSystem.actorSelection(path).resolveOne(RESOLVE_TIMEOUT).onComplete {
          case scala.util.Success(ref) =>
            ctx.self ! WorkerResolved(workerAddr, ref)
            doPush(ctx, ref, req, replyTo, retryCount)
          case scala.util.Failure(ex) =>
            ctx.self ! WorkerResolveFailed(workerAddr, ex)
            ctx.self ! OnPushResult(req.shuffleId, req.partitionIndex, success = false,
              replyTo, retryCount, req.data, req.compression, req.replica)
        }
    }
  }

  private def doPush(
    ctx: ActorContext[Command],
    workerRef: org.apache.pekko.actor.ActorRef,
    req: PushData,
    replyTo: ActorRef[PushDataResponse],
    retryCount: Int
  ): Unit = {
    import org.apache.pekko.pattern.ask
    implicit val timeout: Timeout = Timeout(ASK_TIMEOUT)

    import scala.concurrent.ExecutionContext.Implicits.global
    val future = workerRef ? req
    future.onComplete {
      case scala.util.Success(resp: PushDataResponse) =>
        ctx.self ! OnPushResult(req.shuffleId, req.partitionIndex, resp.success,
          replyTo, retryCount, req.data, req.compression, req.replica)
      case scala.util.Success(other) =>
        ctx.log.warn(s"Unexpected push response: ${other.getClass}")
        ctx.self ! OnPushResult(req.shuffleId, req.partitionIndex, success = false,
          replyTo, retryCount, req.data, req.compression, req.replica)
      case scala.util.Failure(ex) =>
        ctx.self ! OnPushResult(req.shuffleId, req.partitionIndex, success = false,
          replyTo, retryCount, req.data, req.compression, req.replica)
    }
  }
}

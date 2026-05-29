package cn.xuyinyin.meteor.worker

import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.receptionist.Receptionist
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import scala.concurrent.duration._

import cn.xuyinyin.meteor.common.{Protocol, ServiceKeys, MasterActorCommand, Codec, Metrics}
import Protocol._
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.collection.concurrent.TrieMap
import cn.xuyinyin.meteor.transport.{TransportClient, TransportCodec, TransportServer}

/**
 * Meteor Worker — 数据面核心
 *
 * 启动后通过 Pekko Receptionist 发现 Master，注册然后定期心跳。
 *
 * 副本同步：通过 actorSelection 解析远程 Worker ref，
 * 利用 Pekko Artery Remoting 的跨节点通信能力。
 *
 * 对标 Celeborn Worker:
 *   - PartitionDataWriter: 每个 partition 映射一个磁盘文件
 *   - 副本同步: Primary → Replica Worker (Pekko Remoting)
 *   - 多盘 free-space-aware 负载均衡
 *   - Prometheus 指标打点
 */
object WorkerActor {

  sealed trait Command

  // ===== 数据面 =====
  final case class HandlePushData(req: PushData, replyTo: ActorRef[PushDataResponse]) extends Command
  final case class HandleFetchData(req: FetchData, replyTo: ActorRef[FetchDataResponse]) extends Command
  final case class HandleReplicateData(req: ReplicateData, replyTo: ActorRef[ReplicateDataResponse]) extends Command

  // ===== 流式/分块 Fetch =====
  final case class HandleFetchChunk(req: FetchChunk, replyTo: ActorRef[FetchChunkResponse]) extends Command
  final case class HandleFetchChunkRange(req: FetchChunkRange, replyTo: ActorRef[FetchChunkRangeResponse]) extends Command

  /** 从副本恢复数据（故障转移时 Master 通知新 Primary 去 replica 拉数据） */
  final case class HandleRecoverFromReplica(
    req: RecoverFromReplica,
    replyTo: ActorRef[RecoverFromReplicaResponse]
  ) extends Command

  /** 异步副本同步完成回调 */
  private final case class InternalReplicaAck(
    partitionKey: String,
    success: Boolean,
    clientReplyTo: ActorRef[PushDataResponse],
    shuffleId: ShuffleId,
    partitionIndex: Int
  ) extends Command

  // ===== 控制面 =====
  final case class HandleReleaseSlots(req: ReleaseSlots) extends Command

  // ===== 两阶段提交 =====
  final case class HandleCommitShuffle(req: CommitShuffle, replyTo: ActorRef[CommitShuffleResponse]) extends Command

  // ===== 分区提交（标记完成，允许 Fetch） =====
  final case class HandleCommitPartition(shuffleId: ShuffleId, partitionIndex: Int, attemptNumber: Int) extends Command

  // ===== 状态查询 =====
  final case class PartitionInfo(
    key: String,
    size: Long,
    isComplete: Boolean
  )
  final case class QueryPartitions(replyTo: ActorRef[PartitionsQueryResponse]) extends Command
  final case class PartitionsQueryResponse(partitions: Seq[PartitionInfo])

  // ===== 优雅关闭 =====
  case object GracefulShutdown extends Command

  // ===== 磁盘驱逐 =====
  final case class EvictPartitions(bytesToFree: Long) extends Command

  // ===== 磁盘状态更新（来自 DiskMonitor） =====
  private final case class DiskStatusUpdated(status: DiskMonitor.DiskStatus) extends Command

  // ===== 内部 =====
  private case object SendHeartbeat extends Command
  private case object TryRegister extends Command
  private final case class MasterFound(listing: Receptionist.Listing) extends Command
  private final case class RegistrationSuccess(response: RegisterWorkerResponse) extends Command
  private case object ShutdownComplete extends Command

  /** 副本 Worker ref 解析完成 */
  private final case class ReplicaResolved(
    data: ReplicateData,
    ref: ActorRef[Command]
  ) extends Command

  private val nextWorkerId = new AtomicInteger(1)

  /** S3/MinIO 存储配置 */
  final case class S3Config(
    endpoint: String,         // MinIO: http://localhost:9000, AWS: https://s3.amazonaws.com
    accessKey: String,
    secretKey: String,
    bucket: String,
    region: String = "us-east-1",
    enabled: Boolean = true   // 是否启用 S3 二级存储
  )

  /**
   * @param storageDirs 数据存储目录列表（多盘做 free-space-aware 选择）
   * @param host        本机 IP
   * @param port        数据端口
   * @param splitThreshold 分区拆分阈值（字节），默认 256MB
   * @param s3Config    可选的 S3/MinIO 配置（启用二级存储）
   */
  def apply(
    storageDirs: Seq[String],
    host: String = "127.0.0.1",
    port: Int = 9000,
    splitThreshold: Long = 268435456L,   // 256MB
    dataPort: Int = 0,  // 0 = 禁用 Netty transport，>0 = 启用
    rpcPort: Int = 0,
    s3Config: Option[S3Config] = None,   // S3/MinIO 配置
    flushInterval: FiniteDuration = 5.seconds  // 周期性刷盘间隔
  ): Behavior[Command] = Behaviors.setup { ctx =>
    ctx.log.info(s"=== Meteor Worker starting on $host:$port ===")
    ctx.log.info(s"Storage dirs: ${storageDirs.mkString(", ")} (free-space-aware)")
    if (dataPort > 0) ctx.log.info(s"Netty transport enabled on dataPort=$dataPort")

    storageDirs.foreach(d => Files.createDirectories(Paths.get(d)))

    val partitionData = new ConcurrentHashMap[String, PartitionWriter]()

    val transportClient: Option[TransportClient] =
      if (dataPort > 0) Some(new TransportClient()) else None

    val transportServer: Option[TransportServer] =
      if (dataPort > 0) {
        try {
          Some(new TransportServer(host, dataPort, new PartitionWriterStorageAdapter(storageDirs, partitionData)).start())
        } catch {
          case ex: Exception =>
            ctx.log.error(s"Failed to start Netty transport server on $host:$dataPort, fallback to Pekko: ${ex.getMessage}", ex)
            None
        }
      } else None

    val workerAddress = WorkerAddress(host, if (dataPort > 0) dataPort else port, rpcPort = if (rpcPort > 0) rpcPort else port + 1)

    // 发现 Master
    val listingAdapter = ctx.messageAdapter[Receptionist.Listing](MasterFound.apply)
    ctx.system.receptionist ! Receptionist.Subscribe(ServiceKeys.MasterService, listingAdapter)

    ctx.self ! TryRegister

    // 启动磁盘监控（定时推送磁盘状态 + 高水位驱逐）
    val diskStatusAdapter = ctx.messageAdapter[DiskMonitor.DiskStatus](s => DiskStatusUpdated(s))
    ctx.spawn(
      DiskMonitor(storageDirs, workerRef = ctx.self, statusRef = diskStatusAdapter),
      "disk-monitor"
    )

    // 启动周期性 Flusher（对标 Celeborn Worker Flusher）
    val flusherRef = ctx.spawn(
      PeriodicFlusher(flushInterval = flushInterval),
      "periodic-flusher"
    )

    // 初始化 S3/MinIO 二级存储（可选）
    val s3Storage: Option[S3StorageAdapter] = s3Config.filter(_.enabled).map { cfg =>
      ctx.log.info(s"S3 storage enabled: endpoint=${cfg.endpoint}, bucket=${cfg.bucket}")
      new S3StorageAdapter(cfg.endpoint, cfg.accessKey, cfg.secretKey, cfg.bucket, cfg.region)
    }

    ctx.log.info(s"Worker address: $workerAddress")

    // 缓存磁盘状态（由 DiskMonitor 定时更新）
    val diskStatuses = new TrieMap[String, DiskMonitor.DirStatus]()

    discovering(ctx, storageDirs, master = None, workerAddress,
      partitionData = partitionData,
      partitionSizes = new ConcurrentHashMap(),
      splitCounts = new ConcurrentHashMap(),
      partitionCompression = new ConcurrentHashMap(),
      diskStatuses = diskStatuses,
      shuttingDown = false,
      splitThreshold = splitThreshold,
      transportClient = transportClient,
      transportServer = transportServer,
      s3Storage = s3Storage,
      flusherRef = flusherRef)
  }

  // ================================
  // 发现 Master 状态
  // ================================
  private def discovering(
    ctx: ActorContext[Command],
    storageDirs: Seq[String],
    master: Option[ActorRef[MasterActorCommand]],
    address: WorkerAddress,
    partitionData: ConcurrentHashMap[String, PartitionWriter],
    partitionSizes: ConcurrentHashMap[String, AtomicLong],
    splitCounts: ConcurrentHashMap[String, Int],
    partitionCompression: ConcurrentHashMap[String, CompressionType],
    diskStatuses: TrieMap[String, DiskMonitor.DirStatus],
    shuttingDown: Boolean,
    splitThreshold: Long,
    transportClient: Option[TransportClient],
    transportServer: Option[TransportServer],
    s3Storage: Option[S3StorageAdapter],
    flusherRef: ActorRef[PeriodicFlusher.Command]
  ): Behavior[Command] = Behaviors.receiveMessage {

    case MasterFound(listing) =>
      listing.serviceInstances(ServiceKeys.MasterService).headOption match {
        case Some(masterRef) =>
          ctx.log.info("[Discovery] Master found!")
          val newMaster = Some(masterRef.asInstanceOf[ActorRef[MasterActorCommand]])
          ctx.self ! TryRegister
          discovering(ctx, storageDirs, newMaster, address, partitionData, partitionSizes, splitCounts, partitionCompression, diskStatuses, shuttingDown, splitThreshold, transportClient, transportServer, s3Storage, flusherRef)
        case None =>
          Behaviors.same
      }

    case TryRegister =>
      master match {
        case Some(m) =>
          val workerId = s"${address.host}:${address.port}:${nextWorkerId.getAndIncrement()}"
          ctx.log.info(s"[Register] sending RegisterWorker to Master, workerId=$workerId")
          val replyAdapter = ctx.messageAdapter[RegisterWorkerResponse](_ => RegistrationSuccess(
            RegisterWorkerResponse(success = true, workerId)))
          m ! MasterActorCommand.WrappedRegisterWorker(
            RegisterWorker(address, diskSlots = storageDirs.size * 4, memorySlots = 128),
            replyAdapter
          )
          active(ctx, storageDirs, master, address, workerId, partitionData, partitionSizes, splitCounts, partitionCompression, diskStatuses, shuttingDown, splitThreshold, transportClient, transportServer, s3Storage, flusherRef)
        case None =>
          ctx.scheduleOnce(5.seconds, ctx.self, TryRegister)
          Behaviors.same
      }

    case RegistrationSuccess(resp) =>
      ctx.log.info(s"[Register] success! workerId=${resp.workerId}")
      active(ctx, storageDirs, master, address, resp.workerId, partitionData, partitionSizes, splitCounts, partitionCompression, diskStatuses, shuttingDown, splitThreshold, transportClient, transportServer, s3Storage, flusherRef)

    case DiskStatusUpdated(status) =>
      // 更新磁盘状态缓存
      status.storageDirs.foreach(d => diskStatuses.put(d.path, d))
      discovering(ctx, storageDirs, master, address, partitionData, partitionSizes, splitCounts, partitionCompression, diskStatuses, shuttingDown, splitThreshold, transportClient, transportServer, s3Storage, flusherRef)

    case GracefulShutdown =>
      ctx.log.info("Worker shutdown requested (not yet registered)")
      transportClient.foreach(_.close())
      transportServer.foreach(_.close())
      s3Storage.foreach(_.close())
      Behaviors.stopped

    case _ =>
      Behaviors.same
  }

  // ================================
  // 活跃状态
  // ================================
  private def active(
    ctx: ActorContext[Command],
    storageDirs: Seq[String],
    master: Option[ActorRef[MasterActorCommand]],
    address: WorkerAddress,
    workerId: String,
    partitionData: ConcurrentHashMap[String, PartitionWriter],
    partitionSizes: ConcurrentHashMap[String, AtomicLong],
    splitCounts: ConcurrentHashMap[String, Int],
    partitionCompression: ConcurrentHashMap[String, CompressionType],
    diskStatuses: TrieMap[String, DiskMonitor.DirStatus],
    shuttingDown: Boolean,
    splitThreshold: Long,
    transportClient: Option[TransportClient],
    transportServer: Option[TransportServer],
    s3Storage: Option[S3StorageAdapter],
    flusherRef: ActorRef[PeriodicFlusher.Command]
  ): Behavior[Command] = {

    ctx.scheduleOnce(10.seconds, ctx.self, SendHeartbeat)

    Behaviors.receiveMessage {

      // ===== 心跳 =====
      case SendHeartbeat =>
        master.foreach { m =>
          m ! MasterActorCommand.WrappedHeartbeat(
            Heartbeat(workerId, address,
              diskSlotsFree = if (shuttingDown) 0 else storageDirs.size * 4,
              memorySlotsFree = if (shuttingDown) 0 else 128)
          )
          // 指标：更新存活 Worker 数（由 Master 记录，这里记录心跳发送次数）
          Metrics.WorkerMetrics.pushDataRequests.inc(0)  // no-op for heartbeat
        }
        ctx.scheduleOnce(10.seconds, ctx.self, SendHeartbeat)
        Behaviors.same

      // ===== PushData（含 Checksum 校验 + 同步/异步副本 + 周期性刷盘注册 + S3 写回） =====
      case HandlePushData(req, replyTo) =>
        val startNanos = System.nanoTime()
        Metrics.WorkerMetrics.pushDataRequests.inc()

        val key = partitionKey(req.shuffleId, req.partitionIndex, req.attemptNumber)
        try {
          // 0. CRC32 校验（如果客户端传了 checksum）
          val checksumValid = if (req.checksum != 0) {
            val computed = Codec.crc32(req.data)
            if (computed != req.checksum.toInt) {
              ctx.log.error(s"PushData checksum mismatch: key=$key, expected=${req.checksum}, actual=$computed")
              replyTo ! PushDataResponse(req.shuffleId, req.partitionIndex, success = false)
              Metrics.WorkerMetrics.pushDataFailures.inc()
              false
            } else true
          } else true

          if (checksumValid) {
            // 1. 本地写入（free-space-aware 选盘）
            val subIdx = splitCounts.getOrDefault(key, 0)
            val writeKey = if (subIdx > 0) s"${key}:_$subIdx" else key
            val writer = partitionData.computeIfAbsent(writeKey, _ => {
              val dir = selectStorageDirFreeSpaceAware(storageDirs, diskStatuses, req.shuffleId, req.partitionIndex)
              new PartitionWriter(dir, req.shuffleId, req.partitionIndex, req.attemptNumber)
            })

            val dataToStore = req.data
            writer.write(dataToStore)
            if (req.compression != CompressNone) {
              partitionCompression.put(writeKey, req.compression)
            }

            // 注册到周期性 Flusher
            flusherRef ! PeriodicFlusher.RegisterWriter(writeKey, writer)

            Metrics.WorkerMetrics.pushDataBytes.inc(req.data.length)
            Metrics.WorkerMetrics.diskUsage.inc(req.data.length)

            // S3 写回（异步，可选）
            s3Storage.foreach { s3 =>
              import scala.concurrent.ExecutionContext.Implicits.global
              ctx.executionContext.execute(() => {
                try { s3.write(writeKey, dataToStore) }
                catch { case e: Exception => ctx.log.warn(s"S3 write-back failed for $writeKey: ${e.getMessage}") }
              })
            }

            // 2. 拆分检测
            val currentSize = partitionSizes.computeIfAbsent(writeKey, _ => new AtomicLong(0)).addAndGet(req.data.length)
            if (currentSize > splitThreshold && splitThreshold > 0) {
              val newCount = splitCounts.compute(key, (_, v) => if (v == 0) 1 else v + 1)
              partitionSizes.remove(writeKey)
              ctx.log.info(s"Partition split: ${req.shuffleId}[${req.partitionIndex}] → $newCount sub-partitions (${currentSize} bytes)")
              master.foreach { m =>
                m ! MasterActorCommand.WrappedReportPartitionSplit(
                  ReportPartitionSplit(req.shuffleId, req.partitionIndex, newCount, splitThreshold)
                )
              }
            }

            // 3. 副本同步：支持同步/异步两种模式
            val needsSyncReplica = req.requireSyncReplica && req.replica.isDefined

            if (req.replica.isDefined) {
              val replicaAddr = req.replica.get
              transportClient match {
                case Some(client) =>
                  val body = TransportCodec.PushBody(
                    appId = req.shuffleId.appId,
                    shuffleIndex = req.shuffleId.shuffleNum,
                    partitionIndex = req.partitionIndex,
                    attemptNumber = req.attemptNumber,
                    data = req.data,
                    checksum = req.checksum,
                    compression = 0
                  )
                  import scala.concurrent.ExecutionContext.Implicits.global
                  val replicaFuture = client.replicate(replicaAddr.host, replicaAddr.port, body)

                  if (needsSyncReplica) {
                    // 同步副本模式：等待副本确认再 ack client，消除数据丢失窗口
                    replicaFuture.onComplete { result =>
                      val success = result.isSuccess && result.get
                      if (!success) ctx.log.warn(s"Sync replica failed to ${replicaAddr.host}:${replicaAddr.port}")
                      replyTo ! PushDataResponse(req.shuffleId, req.partitionIndex, success = true)
                    }(ctx.executionContext)
                  } else {
                    // 异步副本模式：先 ack client，副本后台完成
                    replicaFuture.foreach { success =>
                      if (!success) ctx.log.warn(s"Async replica failed to ${replicaAddr.host}:${replicaAddr.port}")
                    }(ctx.executionContext)
                    replyTo ! PushDataResponse(req.shuffleId, req.partitionIndex, success = true)
                  }

                case None =>
                  if (needsSyncReplica) {
                    // Pekko fallback: 也需要等副本确认
                    val repData = ReplicateData(req.shuffleId, req.partitionIndex, req.attemptNumber, req.data)
                    val path = s"pekko://meteor-worker@${replicaAddr.host}:${replicaAddr.rpcPort}/user/worker"
                    import org.apache.pekko.actor.typed.scaladsl.adapter._
                    val classicSystem = ctx.system.toClassic
                    classicSystem.actorSelection(path).resolveOne(5.seconds).onComplete {
                      case scala.util.Success(ref) =>
                        ctx.self ! ReplicaResolved(repData, ref)
                        // 不在这里 ack，等 ReplicaAck 回调
                      case scala.util.Failure(ex) =>
                        ctx.log.warn(s"Failed to resolve replica worker: $path - ${ex.getMessage}")
                        replyTo ! PushDataResponse(req.shuffleId, req.partitionIndex, success = true) // 本地已写入
                    }(ctx.executionContext)
                  } else {
                    // 异步副本
                    val repData = ReplicateData(req.shuffleId, req.partitionIndex, req.attemptNumber, req.data)
                    val path = s"pekko://meteor-worker@${replicaAddr.host}:${replicaAddr.rpcPort}/user/worker"
                    import org.apache.pekko.actor.typed.scaladsl.adapter._
                    val classicSystem = ctx.system.toClassic
                    classicSystem.actorSelection(path).resolveOne(5.seconds).onComplete {
                      case scala.util.Success(ref) => ref ! HandleReplicateData(repData, ctx.system.deadLetters)
                      case scala.util.Failure(ex) => ctx.log.warn(s"Failed to resolve replica: $path - ${ex.getMessage}")
                    }(ctx.executionContext)
                    replyTo ! PushDataResponse(req.shuffleId, req.partitionIndex, success = true)
                  }
              }
            } else {
              // 无副本：直接 ack
              replyTo ! PushDataResponse(req.shuffleId, req.partitionIndex, success = true)
            }
          }
        } catch {
          case ex: Exception =>
            ctx.log.error(s"PushData failed: $key - ${ex.getMessage}", ex)
            Metrics.WorkerMetrics.pushDataFailures.inc()
            replyTo ! PushDataResponse(req.shuffleId, req.partitionIndex, success = false)
        } finally {
          val latencySec = (System.nanoTime() - startNanos) / 1e9
          Metrics.WorkerMetrics.pushLatency.observe(latencySec)
        }
        Behaviors.same

      // ===== 副本 ref 解析完成 → 发送 ReplicateData =====
      case ReplicaResolved(data, ref) =>
        ctx.log.debug(s"Sending ReplicateData to ${ref.path}")
        ref ! HandleReplicateData(data, ctx.system.deadLetters)
        Behaviors.same

      // ===== FetchData（指标打点） =====
      case HandleFetchData(req, replyTo) =>
        Metrics.WorkerMetrics.fetchDataRequests.inc()
        val key = partitionKey(req.shuffleId, req.partitionIndex, req.attemptNumber)
        val writer = partitionData.get(key)
        if (writer != null && writer.isComplete) {
          val data = writer.readAll()
          Metrics.WorkerMetrics.fetchDataBytes.inc(data.length)
          replyTo ! FetchDataResponse(req.shuffleId, req.partitionIndex, Some(data))
        } else {
          // 尝试从 S3 恢复
          s3Storage.flatMap(_.readComplete(key)) match {
            case Some(data) =>
              Metrics.WorkerMetrics.fetchDataBytes.inc(data.length)
              replyTo ! FetchDataResponse(req.shuffleId, req.partitionIndex, Some(data))
            case None =>
              replyTo ! FetchDataResponse(req.shuffleId, req.partitionIndex, None)
          }
        }
        Behaviors.same

      // ===== FetchChunk：按 chunk 流式/分块读取 =====
      case HandleFetchChunk(req, replyTo) =>
        val key = partitionKey(req.shuffleId, req.partitionIndex, req.attemptNumber)
        val writer = partitionData.get(key)
        if (writer != null && writer.isComplete) {
          if (req.chunkIndex == -1) {
            // 查询 chunk 总数
            replyTo ! FetchChunkResponse(req.shuffleId, req.partitionIndex, -1, writer.numChunks, Array.emptyByteArray)
          } else {
            writer.readChunk(req.chunkIndex) match {
              case Some(chunkData) =>
                replyTo ! FetchChunkResponse(req.shuffleId, req.partitionIndex, req.chunkIndex, writer.numChunks, chunkData)
              case None =>
                replyTo ! FetchChunkResponse(req.shuffleId, req.partitionIndex, req.chunkIndex, writer.numChunks, Array.emptyByteArray)
            }
          }
        } else {
          // 尝试从 S3 恢复（S3 不支持分块，返回全量作为单 chunk）
          s3Storage.flatMap(_.readComplete(key)) match {
            case Some(data) =>
              replyTo ! FetchChunkResponse(req.shuffleId, req.partitionIndex, 0, 1, data)
            case None =>
              replyTo ! FetchChunkResponse(req.shuffleId, req.partitionIndex, -1, 0, Array.emptyByteArray)
          }
        }
        Behaviors.same

      // ===== FetchChunkRange：批量 chunk 读取（流式拉取） =====
      case HandleFetchChunkRange(req, replyTo) =>
        Metrics.WorkerMetrics.fetchDataRequests.inc()
        val key = partitionKey(req.shuffleId, req.partitionIndex, req.attemptNumber)
        val writer = partitionData.get(key)
        if (writer != null && writer.isComplete) {
          val chunks = (req.startChunk to req.endChunk).flatMap { idx =>
            writer.readChunk(idx)
          }
          var totalBytes = 0L
          chunks.foreach(c => totalBytes += c.length)
          Metrics.WorkerMetrics.fetchDataBytes.inc(totalBytes)
          replyTo ! FetchChunkRangeResponse(req.shuffleId, req.partitionIndex, chunks, writer.numChunks)
        } else {
          replyTo ! FetchChunkRangeResponse(req.shuffleId, req.partitionIndex, Seq.empty, 0)
        }
        Behaviors.same

      // ===== 副本同步：接收远端 ReplicateData =====
      case HandleReplicateData(req, replyTo) =>
        val key = replicateKey(req.shuffleId, req.partitionIndex, req.attemptNumber)
        try {
          val writer = partitionData.computeIfAbsent(key, _ => {
            val dir = selectStorageDirFreeSpaceAware(storageDirs, diskStatuses, req.shuffleId, req.partitionIndex)
            new PartitionWriter(dir, req.shuffleId, req.partitionIndex, req.attemptNumber)
          })
          writer.write(req.data)
          ctx.log.debug(s"Replica written: $key, ${req.data.length} bytes")
          replyTo ! ReplicateDataResponse(success = true)
        } catch {
          case ex: Exception =>
            ctx.log.error(s"ReplicateData failed: $key - ${ex.getMessage}")
            replyTo ! ReplicateDataResponse(success = false)
        }
        Behaviors.same

      // ===== 副本恢复：从 replica Worker 拉数据（Master 通知） =====
      case HandleRecoverFromReplica(req, replyTo) =>
        ctx.log.info(s"RecoverFromReplica: ${req.shuffleId}[${req.partitionIndex}] from ${req.replicaAddress}")

        transportClient match {
          case Some(client) =>
            // Netty 直连路径
            val fetchBody = TransportCodec.FetchBody(
              appId = req.shuffleId.appId,
              shuffleIndex = req.shuffleId.shuffleNum,
              partitionIndex = req.partitionIndex,
              attemptNumber = req.attemptNumber
            )
            import scala.concurrent.ExecutionContext.Implicits.global
            client.fetch(req.replicaAddress.host, req.replicaAddress.port, fetchBody).foreach { data =>
              if (data.nonEmpty) {
                val key = partitionKey(req.shuffleId, req.partitionIndex, req.attemptNumber)
                val writer = partitionData.computeIfAbsent(key, _ => {
                  val dir = selectStorageDirFreeSpaceAware(storageDirs, diskStatuses, req.shuffleId, req.partitionIndex)
                  new PartitionWriter(dir, req.shuffleId, req.partitionIndex, req.attemptNumber)
                })
                writer.write(data)
                writer.close()
                ctx.log.info(s"Recovered ${data.length} bytes via Netty from ${req.replicaAddress.host}:${req.replicaAddress.port}")
                replyTo ! RecoverFromReplicaResponse(req.shuffleId, req.partitionIndex, success = true, dataSize = data.length)
              } else {
                ctx.log.warn(s"Replica data not found for ${req.shuffleId}[${req.partitionIndex}]")
                replyTo ! RecoverFromReplicaResponse(req.shuffleId, req.partitionIndex, success = false)
              }
            }

          case None =>
            // Pekko fallback
            val path = s"pekko://meteor-worker@${req.replicaAddress.host}:${req.replicaAddress.rpcPort}/user/worker"
            import org.apache.pekko.actor.typed.scaladsl.adapter._
            val classicSystem = ctx.system.toClassic
            import scala.concurrent.ExecutionContext.Implicits.global

            classicSystem.actorSelection(path).resolveOne(10.seconds).onComplete {
              case scala.util.Success(ref) =>
                ctx.log.info(s"Resolved replica Worker: $path, sending FetchData")
                val fetchProbe = ctx.spawnAnonymous(
                  org.apache.pekko.actor.typed.scaladsl.Behaviors.receiveMessage[FetchDataResponse] { resp =>
                    resp.data match {
                      case Some(data) =>
                        val key = partitionKey(req.shuffleId, req.partitionIndex, req.attemptNumber)
                        val writer = partitionData.computeIfAbsent(key, _ => {
                          val dir = selectStorageDirFreeSpaceAware(storageDirs, diskStatuses, req.shuffleId, req.partitionIndex)
                          new PartitionWriter(dir, req.shuffleId, req.partitionIndex, req.attemptNumber)
                        })
                        writer.write(data)
                        writer.close()
                        ctx.log.info(s"Recovered ${data.length} bytes for ${req.shuffleId}[${req.partitionIndex}] from replica")
                        replyTo ! RecoverFromReplicaResponse(req.shuffleId, req.partitionIndex, success = true, dataSize = data.length)

                      case None =>
                        ctx.log.warn(s"Replica has no data for ${req.shuffleId}[${req.partitionIndex}]")
                        replyTo ! RecoverFromReplicaResponse(req.shuffleId, req.partitionIndex, success = false)
                    }
                    org.apache.pekko.actor.typed.scaladsl.Behaviors.stopped
                  }
                )

                val typedRef = org.apache.pekko.actor.typed.ActorRefResolver(ctx.system)
                  .resolveActorRef[Command](path)
                typedRef ! HandleFetchData(
                  FetchData(req.shuffleId, req.partitionIndex, req.attemptNumber),
                  fetchProbe
                )

              case scala.util.Failure(ex) =>
                ctx.log.warn(s"Failed to resolve replica worker: $path - ${ex.getMessage}")
                replyTo ! RecoverFromReplicaResponse(req.shuffleId, req.partitionIndex, success = false)
            }(ctx.executionContext)
        }
        Behaviors.same

      // ===== 释放 Slot（指标打点） =====
      case HandleReleaseSlots(req) =>
        val prefix = s"${req.shuffleId.appId}:${req.shuffleId.shuffleNum}"
        val keys = partitionData.keys()
        while (keys.hasMoreElements) {
          val key = keys.nextElement()
          if (key.startsWith(prefix)) {
            val removed = partitionData.remove(key)
            if (removed != null) {
              val sz = removed.size
              removed.close()
              // 释放磁盘用量
              Metrics.WorkerMetrics.diskUsage.dec(sz)
              // 删除磁盘文件
              val dir = selectStorageDirFreeSpaceAware(storageDirs, diskStatuses, req.shuffleId, 0)
              val filePath = dir.resolve(s"$key.data")
              if (Files.exists(filePath)) {
                try Files.delete(filePath)
                catch { case _: Exception => }
              }
            }
          }
        }
        Metrics.WorkerMetrics.partitionWriters.set(partitionData.size())
        ctx.log.info(s"Released partitions for shuffle ${req.shuffleId}, remaining writers: ${partitionData.size()}")
        Behaviors.same

      // ===== 分区提交（指标打点） =====
      case HandleCommitPartition(shuffleId, partitionIndex, attemptNumber) =>
        val key = partitionKey(shuffleId, partitionIndex, attemptNumber)
        val writer = partitionData.get(key)
        if (writer != null) {
          writer.close()
          ctx.log.info(s"Committed partition $key, size=${writer.size}")
          // 通知 Master：分区已提交
          master.foreach { m =>
            m ! MasterActorCommand.WrappedPartitionCommitted(
              Protocol.PartitionCommitted(shuffleId, partitionIndex, workerId, writer.size))
          }
        } else {
          ctx.log.warn(s"CommitPartition: no writer for $key")
        }
        Metrics.WorkerMetrics.partitionWriters.set(partitionData.size())
        Behaviors.same

      // ===== CommitShuffle（两阶段提交阶段二）：Master 通知所有 mapper 已完成 =====
      case HandleCommitShuffle(req, replyTo) =>
        val prefix = s"${req.shuffleId.appId}:${req.shuffleId.shuffleNum}"
        ctx.log.info(s"CommitShuffle triggered for $prefix: flushing and finalizing all partitions")

        // 1. 找到该 shuffle 的所有 partition writer
        var finalizedCount = 0
        val keys = partitionData.keys()
        while (keys.hasMoreElements) {
          val key = keys.nextElement()
          if (key.startsWith(prefix)) {
            val writer = partitionData.get(key)
            if (writer != null && !writer.isComplete) {
              try {
                writer.close()  // flush + fsync + mark complete
                // 注销周期性 flusher
                flusherRef ! PeriodicFlusher.UnregisterWriter(key)
                finalizedCount += 1
              } catch {
                case e: Exception =>
                  ctx.log.error(s"Error finalizing $key during CommitShuffle: ${e.getMessage}")
              }
            }
          }
        }

        // 2. 通知 Master 提交结果
        ctx.log.info(s"CommitShuffle complete: $finalizedCount partitions finalized for $prefix")
        replyTo ! CommitShuffleResponse(req.shuffleId, success = true)
        Behaviors.same

      // ===== 磁盘状态更新 =====
      case DiskStatusUpdated(status) =>
        status.storageDirs.foreach(d => diskStatuses.put(d.path, d))
        Behaviors.same

      // ===== 发现态回音（活跃态静默） =====
      case MasterFound(_) | RegistrationSuccess(_) | TryRegister | InternalReplicaAck(_, _, _, _, _) =>
        Behaviors.same

      // ===== 状态查询 =====
      case QueryPartitions(replyTo) =>
        val infos = {
          val keys = partitionData.keys()
          val buf = scala.collection.mutable.ArrayBuffer[PartitionInfo]()
          while (keys.hasMoreElements) {
            val key = keys.nextElement()
            val writer = partitionData.get(key)
            if (writer != null) {
              buf += PartitionInfo(key, writer.size, writer.isComplete)
            }
          }
          buf.toSeq
        }
        replyTo ! PartitionsQueryResponse(infos)
        Behaviors.same

      // ===== 优雅关闭（通知 Master 注销 → 排空 Writers → 停止） =====
      case GracefulShutdown =>
        ctx.log.info(s"Worker $workerId shutting down gracefully. Flushing ${partitionData.size()} partitions...")

        // 1. 通知 Master：注销 Worker
        master.foreach { m =>
          m ! MasterActorCommand.WrappedUnregisterWorker(
            UnregisterWorker(workerId, reason = s"Graceful shutdown from $workerId")
          )
          ctx.log.info(s"[GracefulShutdown] Sent UnregisterWorker to Master")
        }

        // 2. 排空所有 partition writers
        val keys = partitionData.keys()
        while (keys.hasMoreElements) {
          val key = keys.nextElement()
          val writer = partitionData.get(key)
          if (writer != null) {
            try {
              writer.close()
              Metrics.WorkerMetrics.diskUsage.dec(writer.size)
            } catch {
              case e: Exception =>
                ctx.log.error(s"Error closing writer for $key: ${e.getMessage}")
            }
          }
        }
        partitionData.clear()
        transportClient.foreach(_.close())
        transportServer.foreach(_.close())
        s3Storage.foreach(_.close())
        Metrics.WorkerMetrics.partitionWriters.set(0)
        ctx.log.info(s"Worker $workerId shutdown complete.")
        Behaviors.stopped

      // ===== 注销完成（Master 已确认） =====
      case ShutdownComplete =>
        ctx.log.info(s"Worker $workerId: Master acknowledged unregister, stopping.")
        Behaviors.stopped

      // ===== 磁盘驱逐（LRU：驱逐已完成的 partition） =====
      case EvictPartitions(bytesToFree) =>
        ctx.log.info(s"Eviction triggered, need to free ${bytesToFree} bytes")
        var freed = 0L
        val toEvict = scala.collection.mutable.ArrayBuffer[String]()

        // 收集已完成的 partition，按 key 排序（FIFO，可改 LRU）
        val completed = {
          val keys = partitionData.keys()
          val buf = scala.collection.mutable.ArrayBuffer[(String, PartitionWriter)]()
          while (keys.hasMoreElements) {
            val key = keys.nextElement()
            val writer = partitionData.get(key)
            if (writer != null && writer.isComplete) {
              buf += ((key, writer))
            }
          }
          buf.sortBy(_._1)
        }

        for ((key, writer) <- completed if freed < bytesToFree) {
          try {
            freed += writer.size
            Metrics.WorkerMetrics.diskUsage.dec(writer.size)
            writer.close()
            // 删除磁盘文件
            val path = Paths.get(storageDirs.head).resolve(key.replace(":", "_") + ".data")
            if (Files.exists(path)) {
              try Files.delete(path)
              catch { case _: Exception => }
            }
            toEvict += key
          } catch {
            case e: Exception =>
              ctx.log.error(s"Error evicting $key: ${e.getMessage}")
          }
        }

        toEvict.foreach(partitionData.remove)
        Metrics.WorkerMetrics.partitionWriters.set(partitionData.size())
        ctx.log.info(s"Evicted ${toEvict.size} partitions, freed ${freed} bytes (${partitionData.size()} remaining)")
        Behaviors.same
    }
  }

  // ================================
  // 工具
  // ================================

  def partitionKey(shuffleId: ShuffleId, partitionIndex: Int, attemptNumber: Int): String =
    s"${shuffleId.appId}:${shuffleId.shuffleNum}:$partitionIndex:$attemptNumber"

  def replicateKey(shuffleId: ShuffleId, partitionIndex: Int, attemptNumber: Int): String =
    s"${shuffleId.appId}:${shuffleId.shuffleNum}:$partitionIndex:$attemptNumber:replica"

  /** 旧版 round-robin 选盘（保留作 fallback） */
  private def selectStorageDir(dirs: Seq[String], shuffleId: ShuffleId, partitionIndex: Int): Path = {
    val dir = dirs(partitionIndex % dirs.size)
    val path = Paths.get(dir, shuffleId.appId, shuffleId.shuffleNum.toString)
    Files.createDirectories(path)
    path
  }

  /**
   * Free-space-aware 选盘
   *
   * 优先选可用空间最多的盘（加权轮询），避免热点。
   * 如果 DiskMonitor 尚未采集数据，fallback 到 round-robin。
   */
  private def selectStorageDirFreeSpaceAware(
    dirs: Seq[String],
    diskStatuses: TrieMap[String, DiskMonitor.DirStatus],
    shuffleId: ShuffleId,
    partitionIndex: Int
  ): Path = {
    if (diskStatuses.isEmpty) {
      return selectStorageDir(dirs, shuffleId, partitionIndex)
    }

    // 找可用空间最多的盘（加权：freeSpace 越大权重越高）
    val candidates = dirs.flatMap(d => diskStatuses.get(d).map((d, _)))
    if (candidates.isEmpty) {
      selectStorageDir(dirs, shuffleId, partitionIndex)
    } else {
      // 选 freeSpace 最大的盘
      val best = candidates.maxBy(_._2.freeSpace)
      val path = Paths.get(best._1, shuffleId.appId, shuffleId.shuffleNum.toString)
      Files.createDirectories(path)
      path
    }
  }
}

// ================================
// PartitionWriter — Chunked mmap I/O
// ================================
//
// 写入路径：direct ByteBuffer → FileChannel.write（避免 heap→direct 拷贝）
// 读取路径：FileChannel.map (mmap) → 内核直接映射（zero-copy 读）
// 分块存储：自动按 64MB 分块，避免单文件超大 + Pekko 消息限制
//
// 对标 Celeborn 的 ChunkManager：
//   - chunkSize: 64MB（可配置）
//   - 文件命名：{partition}.{attempt}.{chunk}.data
class PartitionWriter(
  dir: Path,
  shuffleId: ShuffleId,
  partitionIndex: Int,
  attemptNumber: Int,
  chunkSize: Long = 64L * 1024 * 1024   // 64MB per chunk
) {
  // Chunk 文件列表：每个 chunk 是独立的 RandomAccessFile + FileChannel
  private case class ChunkFile(index: Int, raf: java.io.RandomAccessFile, channel: java.nio.channels.FileChannel, size: Long)
  private val chunks = scala.collection.mutable.ArrayBuffer[ChunkFile]()
  private var currentChunkIdx = -1
  @volatile private var totalSize = 0L
  @volatile private var complete = false

  private def chunkPath(idx: Int): Path =
    dir.resolve(s"$partitionIndex.$attemptNumber.$idx.data")

  /** 确保有写入用的活跃 chunk */
  private def ensureChunk(): ChunkFile = {
    if (currentChunkIdx >= 0 && chunks.nonEmpty && chunks.last.size < chunkSize) {
      return chunks.last
    }
    val nextIdx = chunks.size
    val path = chunkPath(nextIdx)
    val raf = new java.io.RandomAccessFile(path.toFile, "rw")
    val ch = raf.getChannel
    val cf = ChunkFile(nextIdx, raf, ch, 0L)
    chunks += cf
    currentChunkIdx = nextIdx
    cf
  }

  def write(data: Array[Byte]): Unit = {
    var remaining = data
    while (remaining.nonEmpty) {
      val ch = ensureChunk()
      val spaceLeft = (chunkSize - ch.size).toInt
      val toWrite = if (remaining.length <= spaceLeft) remaining else remaining.take(spaceLeft)

      val buf = java.nio.ByteBuffer.allocateDirect(toWrite.length)
      buf.put(toWrite)
      buf.flip()
      while (buf.hasRemaining) {
        ch.channel.write(buf)
      }

      // 更新当前 chunk 大小
      val updatedIdx = chunks.indexWhere(_.index == ch.index)
      if (updatedIdx >= 0) {
        chunks(updatedIdx) = ch.copy(size = ch.size + toWrite.length)
      }

      totalSize += toWrite.length
      remaining = remaining.drop(toWrite.length)
    }
  }

  /** 写入压缩帧（带 Codec 帧头） */
  def writeCompressed(data: Array[Byte], algo: Codec.Algorithm): Unit = {
    val frame = Codec.compress(data, algo)
    write(frame)
  }

  /**
   * 读取全部数据（自动合并所有 chunk）
   * 使用 mmap 逐 chunk 拼接
   */
  def readAll(): Array[Byte] = {
    if (totalSize == 0) return Array.emptyByteArray
    val result = new Array[Byte](totalSize.toInt)
    var offset = 0
    for (ch <- chunks) {
      if (ch.size > 0) {
        val readRaf = new java.io.RandomAccessFile(chunkPath(ch.index).toFile, "r")
        try {
          val readChannel = readRaf.getChannel
          val mapped = readChannel.map(
            java.nio.channels.FileChannel.MapMode.READ_ONLY, 0, ch.size)
          mapped.get(result, offset, ch.size.toInt)
          offset += ch.size.toInt
        } finally {
          readRaf.close()
        }
      }
    }
    result
  }

  /** 按 chunk 读取（用于分块传输） */
  def readChunk(chunkIndex: Int): Option[Array[Byte]] = {
    if (chunkIndex < 0 || chunkIndex >= chunks.size) return None
    val ch = chunks(chunkIndex)
    if (ch.size == 0) return Some(Array.emptyByteArray)
    val readRaf = new java.io.RandomAccessFile(chunkPath(ch.index).toFile, "r")
    try {
      val readChannel = readRaf.getChannel
      val mapped = readChannel.map(
        java.nio.channels.FileChannel.MapMode.READ_ONLY, 0, ch.size)
      val data = new Array[Byte](ch.size.toInt)
      mapped.get(data)
      Some(data)
    } finally {
      readRaf.close()
    }
  }

  def numChunks: Int = chunks.size
  def chunkSize(idx: Int): Long = if (idx >= 0 && idx < chunks.size) chunks(idx).size else 0L

  /** 读取并解压（自动检测是否为 Meteor 帧） */
  def readAllDecompressed(): Array[Byte] = {
    val raw = readAll()
    if (Codec.isMeteorFrame(raw)) Codec.decompress(raw) else raw
  }

  def size: Long = totalSize
  def isComplete: Boolean = complete

  def close(): Unit = {
    if (complete) return
    complete = true
    flush()
    for (ch <- chunks) {
      ch.channel.close()
      ch.raf.close()
    }
  }

  /** 刷盘但不关闭（周期性 fsync） */
  def flush(): Unit = {
    for (ch <- chunks) {
      if (ch.size > 0) {
        ch.channel.force(false)  // false = 只刷数据不刷元数据（更快）
      }
    }
  }
}

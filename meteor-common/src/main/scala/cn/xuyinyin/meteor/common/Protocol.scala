package cn.xuyinyin.meteor.common

/**
 * Meteor Shuffle 协议定义
 *
 * 映射 Celeborn 的核心概念：
 *   - ShuffleId: 一次 shuffle 的唯一标识
 *   - PartitionId: shuffle 内的分区
 *   - PartitionLocation: 分区数据所在的 Worker 地址（主 + 从）
 *   - Epoch: 分区版本号，用于故障恢复
 */
object Protocol {

  // ================================
  // 标识符
  // ================================

  /** 一次 shuffle 的全局唯一 ID */
  final case class ShuffleId(appId: String, shuffleNum: Int)

  /** shuffle 内的分区标识 */
  final case class PartitionId(shuffleId: ShuffleId, partitionIndex: Int)

  /** 分区位置：主副本 + 从副本 */
  final case class PartitionLocation(
    id: PartitionId,
    epoch: Int,
    primary: WorkerAddress,
    replica: Option[WorkerAddress]
  )

  /** Worker 网络地址 */
  final case class WorkerAddress(host: String, port: Int, rpcPort: Int) {
    def toPekkoAddress: String = s"pekko://meteor-worker@$host:$rpcPort"
  }

  // ================================
  // Master 协议（Client → Master）
  // ================================

  /** 注册一个 shuffle，请求分区 slot 分配 */
  final case class RegisterShuffle(
    shuffleId: ShuffleId,
    numPartitions: Int
  )

  /** RegisterShuffle 的响应 */
  final case class RegisterShuffleResponse(
    locations: Seq[PartitionLocation]
  )

  /** 请求重新分配某个分区（原 Worker 挂了） */
  final case class Revive(
    shuffleId: ShuffleId,
    partitionIndex: Int,
    epoch: Int,
    cause: Option[String] = None
  )

  /** Revive 的响应：新的 PartitionLocation 或原地不变
   *  @param replicaToRecover 如果原位置有副本，提供副本地址供 Client 恢复数据
   */
  final case class ReviveResponse(
    location: PartitionLocation,
    replicaToRecover: Option[WorkerAddress] = None
  )

  /** 获取已有 shuffle 的分区位置 */
  final case class GetShuffleLocations(shuffleId: ShuffleId)

  final case class GetShuffleLocationsResponse(
    locations: Seq[PartitionLocation]
  )

  // ================================
  // Master 内部协议（Master ↔ Worker）
  // ================================

  /** Worker 向 Master 注册 */
  final case class RegisterWorker(
    address: WorkerAddress,
    diskSlots: Int,       // 可用磁盘 slot 数
    memorySlots: Int      // 可用内存 slot 数
  )

  final case class RegisterWorkerResponse(success: Boolean, workerId: String)

  /** Worker 心跳 */
  final case class Heartbeat(
    workerId: String,
    address: WorkerAddress,
    diskSlotsFree: Int,
    memorySlotsFree: Int
  )

  /** Worker 下线通知 */
  final case class UnregisterWorker(workerId: String, reason: String)

  // ================================
  // 数据面协议（Client ↔ Worker）
  // ================================

  /** 压缩算法枚举（对应 Codec.Algorithm） */
  sealed trait CompressionType { def id: Byte }
  case object CompressNone   extends CompressionType { val id: Byte = -1 }  // 不压缩
  case object CompressLZ4    extends CompressionType { val id: Byte = 0 }
  case object CompressSnappy extends CompressionType { val id: Byte = 1 }
  case object CompressZstd   extends CompressionType { val id: Byte = 2 }

  /** PushData: 上游 TM 推送一个分区的数据块 */
  final case class PushData(
    shuffleId: ShuffleId,
    partitionIndex: Int,
    attemptNumber: Int,
    data: Array[Byte],
    checksum: Long = 0,                      // CRC32 of original uncompressed data
    replica: Option[WorkerAddress] = None,   // 副本同步目标 Worker 地址
    compression: CompressionType = CompressNone  // 数据压缩算法
  )

  /** PushData 响应 */
  final case class PushDataResponse(
    shuffleId: ShuffleId,
    partitionIndex: Int,
    success: Boolean
  )

  /** FetchData: 下游 TM 拉取一个分区的数据 */
  final case class FetchData(
    shuffleId: ShuffleId,
    partitionIndex: Int,
    attemptNumber: Int
  )

  /** FetchData 响应 */
  final case class FetchDataResponse(
    shuffleId: ShuffleId,
    partitionIndex: Int,
    data: Option[Array[Byte]],  // None = 数据还没就绪
    fromReplica: Boolean = false
  )

  // ================================
  // Worker 内部：副本同步
  // ================================

  /** 主副本推送到从副本 */
  final case class ReplicateData(
    shuffleId: ShuffleId,
    partitionIndex: Int,
    attemptNumber: Int,
    data: Array[Byte]
  )

  final case class ReplicateDataResponse(success: Boolean)

  // ================================
  // 生命周期
  // ================================

  /** Master 通知 Worker：某个 shuffle 结束，可以清理数据 */
  final case class ReleaseSlots(
    shuffleId: ShuffleId,
    workerIds: Set[String]
  )

  /** Master 通知所有相关 Worker：shuffle 完成 */
  final case class UnregisterShuffle(shuffleId: ShuffleId)

  // ================================
  // Worker → Master：分区拆分
  // ================================

  /** Worker 通知 Master：分区数据超出阈值，已拆分为子分区 */
  final case class ReportPartitionSplit(
    shuffleId: ShuffleId,
    partitionIndex: Int,
    subPartitions: Int,    // 拆分后的子分区数量
    splitThreshold: Long   // 触发拆分的阈值（字节）
  )

  /** Worker → Master：分区已提交（mapper 写完了） */
  final case class PartitionCommitted(
    shuffleId: ShuffleId,
    partitionIndex: Int,
    workerId: String,
    dataSize: Long          // 分区数据大小
  )

  /** Master → Worker：从副本恢复数据（故障转移时） */
  final case class RecoverFromReplica(
    shuffleId: ShuffleId,
    partitionIndex: Int,
    attemptNumber: Int,
    replicaAddress: WorkerAddress
  )

  final case class RecoverFromReplicaResponse(
    shuffleId: ShuffleId,
    partitionIndex: Int,
    success: Boolean,
    data: Option[Array[Byte]] = None,
    dataSize: Long = 0
  )

  /** Client → Master：查询分区是否已提交（reducer 等待） */
  final case class IsPartitionReady(
    shuffleId: ShuffleId,
    partitionIndex: Int
  )

  final case class IsPartitionReadyResponse(
    ready: Boolean,
    dataSize: Long = 0
  )

  /** Client → Master：报告 Worker 推送失败 */
  final case class ReportWorkerFailure(
    workerId: String,
    shuffleId: ShuffleId,
    partitionIndex: Int,
    errorMessage: String
  )
}

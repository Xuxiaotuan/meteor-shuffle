package cn.xuyinyin.meteor.spi

import org.apache.flink.runtime.shuffle.{
  ShuffleServiceFactory, ShuffleMaster, ShuffleMasterContext,
  ShuffleDescriptor, ShuffleEnvironment, ShuffleEnvironmentContext,
  ShuffleIOOwnerContext,
  PartitionDescriptor, ProducerDescriptor,
  TaskInputsOutputsDescriptor
}
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter
import org.apache.flink.runtime.io.network.partition.consumer.{
  IndexedInputGate, InputChannel, BufferOrEvent
}
import org.apache.flink.runtime.io.network.partition.{
  ResultPartitionID, BufferAvailabilityListener, ResultSubpartitionView
}
import org.apache.flink.runtime.io.network.api.{EndOfData, StopMode}
import org.apache.flink.runtime.deployment.{
  ResultPartitionDeploymentDescriptor, InputGateDeploymentDescriptor
}
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider
import org.apache.flink.runtime.io.network.buffer.{Buffer, NetworkBuffer, FreeingBufferRecycler, BufferBuilder}
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType
import org.apache.flink.core.memory.MemorySegmentFactory
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID
import org.apache.flink.runtime.event.AbstractEvent
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo
import org.apache.flink.api.common.JobID
import org.apache.flink.configuration.{Configuration, MemorySize}
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup
import org.apache.flink.runtime.checkpoint.CheckpointException
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo
import cn.xuyinyin.meteor.client.{ShuffleClient, LifecycleManager}
import cn.xuyinyin.meteor.common.{Protocol, MasterActorCommand, Codec}
import Protocol._

import java.util.concurrent.{CompletableFuture, ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.{util => ju}
import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap

/**
 * Meteor Flink SPI 入口 — 完整实现
 *
 * 数据流：
 *   Mapper.emitRecord → MeteorResultPartitionWriter → ShuffleClient.Push → Worker
 *   Reducer.pollNext   → MeteorIndexedInputGate → ShuffleClient.Fetch → Worker
 *
 * 配置项（flink-conf.yaml）：
 *   meteor.master.host: localhost
 *   meteor.master.port: 7337
 *   meteor.client.push.replicate: false
 */
class MeteorShuffleServiceFactory
  extends ShuffleServiceFactory[
    MeteorShuffleDescriptor,
    MeteorResultPartitionWriter,
    MeteorIndexedInputGate
  ] {

  override def createShuffleMaster(
    context: ShuffleMasterContext
  ): ShuffleMaster[MeteorShuffleDescriptor] = {
    initPlugin(context.getConfiguration)
    new MeteorShuffleMaster()
  }

  override def createShuffleEnvironment(
    context: ShuffleEnvironmentContext
  ): ShuffleEnvironment[MeteorResultPartitionWriter, MeteorIndexedInputGate] = {
    initPlugin(context.getConfiguration)
    new MeteorShuffleEnvironment()
  }

  private def initPlugin(conf: Configuration): Unit = {
    val masterHost = conf.getString("meteor.master.host", "localhost")
    val masterPort = conf.getInteger("meteor.master.port", 7337)
    val replicate = conf.getBoolean("meteor.client.push.replicate", false)
    MeteorPluginContext.init(masterHost, masterPort, replicate)
  }
}

// ================================
// ShuffleDescriptor — 携带 shuffle 位置信息
// ================================

class MeteorShuffleDescriptor(
  val shuffleId: ShuffleId,
  val numPartitions: Int,
  val locations: Map[Int, PartitionLocation],
  val resultPartitionId: Option[ResultPartitionID] = None  // 用于本地 in-memory 数据交换
) extends ShuffleDescriptor {

  override def isUnknown: Boolean = locations.isEmpty

  override def getResultPartitionID: ResultPartitionID =
    resultPartitionId.getOrElse(
      new ResultPartitionID(
        new IntermediateResultPartitionID(),
        org.apache.flink.runtime.executiongraph.ExecutionAttemptID.randomId()
      )
    )

  override def storesLocalResourcesOn: java.util.Optional[org.apache.flink.runtime.clusterframework.types.ResourceID] =
    java.util.Optional.empty()

  override def equals(obj: Any): Boolean = obj match {
    case other: MeteorShuffleDescriptor =>
      shuffleId == other.shuffleId && numPartitions == other.numPartitions
    case _ => false
  }

  override def hashCode(): Int = {
    val prime = 31; var r = 1
    r = prime * r + shuffleId.##; r = prime * r + numPartitions; r
  }

  override def toString: String =
    s"MeteorShuffleDescriptor(shuffleId=$shuffleId, partitions=$numPartitions, locations=${locations.size})"
}

// ================================
// ShuffleMaster — JM 侧注册 shuffle
// ================================

class MeteorShuffleMaster extends ShuffleMaster[MeteorShuffleDescriptor] {

  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  // 缓存最近注册的 descriptor
  private val descriptorCache = TrieMap.empty[String, MeteorShuffleDescriptor]

  override def registerPartitionWithProducer(
    jobID: JobID,
    partitionDescriptor: PartitionDescriptor,
    producerDescriptor: ProducerDescriptor
  ): CompletableFuture[MeteorShuffleDescriptor] = {

    val ipid = partitionDescriptor.getPartitionId
    val execAttemptId = producerDescriptor.getProducerExecutionId
    val rpid = new ResultPartitionID(ipid, execAttemptId)
    val numPartitions = partitionDescriptor.getTotalNumberOfPartitions
    val jobIdHex = jobID.toString
    val shuffleId = ShuffleId(jobIdHex, partitionDescriptor.getPartitionId.hashCode.abs)

    // 如果 Master 不可用，返回带 ResultPartitionID 的 descriptor（走本地 in-memory 路径）
    if (!MeteorPluginContext.isInitialized) {
      log.info(s"Meteor Master not available; using local in-memory exchange for shuffle $shuffleId (rpid=$rpid)")
      val desc = new MeteorShuffleDescriptor(shuffleId, numPartitions, Map.empty, Some(rpid))
      descriptorCache.put(jobIdHex, desc)
      return CompletableFuture.completedFuture(desc)
    }

    val lm = MeteorPluginContext.getLifecycleManager

    import org.apache.pekko.actor.typed.scaladsl.AskPattern._
    import scala.concurrent.duration._
    implicit val timeout: org.apache.pekko.util.Timeout =
      org.apache.pekko.util.Timeout(15.seconds)
    implicit val scheduler: org.apache.pekko.actor.typed.Scheduler =
      MeteorPluginContext.getSystem.scheduler

    import scala.concurrent.Await
    val resp = Await.result(
      lm.ask[RegisterShuffleResponse](replyTo =>
        LifecycleManager.RegisterShuffle(shuffleId, numPartitions, replyTo)),
      15.seconds
    )

    val locations = resp.locations.map(l => l.id.partitionIndex -> l).toMap
    val desc = new MeteorShuffleDescriptor(shuffleId, numPartitions, locations, Some(rpid))
    descriptorCache.put(jobIdHex, desc)
    CompletableFuture.completedFuture(desc)
  }

  override def releasePartitionExternally(descriptor: ShuffleDescriptor): Unit = {
    if (!MeteorPluginContext.isInitialized) return
    descriptor match {
      case md: MeteorShuffleDescriptor =>
        val lm = MeteorPluginContext.getLifecycleManager
        import org.apache.pekko.actor.typed.scaladsl.AskPattern._
        import scala.concurrent.duration._
        implicit val timeout: org.apache.pekko.util.Timeout =
          org.apache.pekko.util.Timeout(10.seconds)
        implicit val scheduler: org.apache.pekko.actor.typed.Scheduler =
          MeteorPluginContext.getSystem.scheduler

        import scala.concurrent.Await
        Await.ready(
          lm.ask[LifecycleManager.UnregisterShuffleResponse](replyTo =>
            LifecycleManager.UnregisterShuffle(md.shuffleId, replyTo)),
          10.seconds
        )

        descriptorCache.retain((k, _) => !k.startsWith(md.shuffleId.appId))
      case _ =>
    }
  }

  override def computeShuffleMemorySizeForTask(
    inputsOutputs: TaskInputsOutputsDescriptor
  ): MemorySize = MemorySize.ofMebiBytes(128)

  override def close(): Unit = {}
}

// ================================
// ShuffleEnvironment — TM 侧创建 reader/writer
// ================================

class MeteorShuffleEnvironment
  extends ShuffleEnvironment[MeteorResultPartitionWriter, MeteorIndexedInputGate] {

  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  override def start(): Int = -1 // -1 = local-only, data port handled by Meteor Worker

  override def createShuffleIOOwnerContext(
    ownerName: String,
    executionAttemptID: org.apache.flink.runtime.executiongraph.ExecutionAttemptID,
    metricGroup: org.apache.flink.metrics.MetricGroup
  ): ShuffleIOOwnerContext =
    new ShuffleIOOwnerContext(ownerName, executionAttemptID, metricGroup, metricGroup, metricGroup)

  override def createResultPartitionWriters(
    ownerContext: ShuffleIOOwnerContext,
    descriptors: ju.List[ResultPartitionDeploymentDescriptor]
  ): ju.List[MeteorResultPartitionWriter] = {
    val attemptId = ownerContext.getExecutionAttemptID
    log.info(s"[MeteorEnv] createResultPartitionWriters owner=${ownerContext.getOwnerName} attemptId=$attemptId descriptors=${descriptors.size()}")
    descriptors.asScala.map { d =>
      val ipid = d.getPartitionId
      val rpid = new ResultPartitionID(ipid, attemptId)
      val numSubs = d.getNumberOfSubpartitions
      val shuffleDesc = d.getShuffleDescriptor match {
        case md: MeteorShuffleDescriptor => Some(md)
        case _ => None
      }
      val writer = new MeteorResultPartitionWriter(ownerContext.getOwnerName, ipid, attemptId, numSubs, shuffleDesc)
      log.info(s"[MeteorEnv] created owner=${ownerContext.getOwnerName} writerId=${System.identityHashCode(writer)} ipid=$ipid rpid=$rpid numSubs=$numSubs shuffleDescClass=${d.getShuffleDescriptor.getClass.getName} shuffleDesc=${shuffleDesc.map(md => s"${md.toString}, resultPartitionId=${md.resultPartitionId}").getOrElse("none")}")
      // 注册到本地 registry（用于 in-memory 数据交换）
      MeteorPluginContext.registerLocalPartition(rpid, writer)
      writer
    }.toList.asJava
  }

  override def releasePartitionsLocally(ids: ju.Collection[ResultPartitionID]): Unit = {}

  override def getPartitionsOccupyingLocalResources: ju.Collection[ResultPartitionID] =
    ju.Collections.emptyList()

  override def createInputGates(
    ownerContext: ShuffleIOOwnerContext,
    partitionProducerStateProvider: PartitionProducerStateProvider,
    descriptors: ju.List[InputGateDeploymentDescriptor]
  ): ju.List[MeteorIndexedInputGate] = {
    log.info(s"[MeteorEnv] createInputGates owner=${ownerContext.getOwnerName} attemptId=${ownerContext.getExecutionAttemptID} descriptors=${descriptors.size()}")
    descriptors.asScala.map { gateDesc =>
      val shuffleDescs = gateDesc.getShuffleDescriptors.toSeq
      val consumedSubIdx = gateDesc.getConsumedSubpartitionIndex
      log.info(s"[MeteorEnv] inputGate descriptor owner=${ownerContext.getOwnerName} shuffleDescs=${shuffleDescs.size} consumedSubpartition=$consumedSubIdx")
      shuffleDescs.foreach {
        case md: MeteorShuffleDescriptor =>
          log.info(s"[MeteorEnv] input shuffleDesc owner=${ownerContext.getOwnerName} shuffleId=${md.shuffleId} resultPartitionId=${md.resultPartitionId} numPartitions=${md.numPartitions}")
        case other =>
          log.info(s"[MeteorEnv] input shuffleDesc owner=${ownerContext.getOwnerName} class=${other.getClass.getName}")
      }
      new MeteorIndexedInputGate(ownerContext, shuffleDescs, consumedSubIdx)
    }.toList.asJava
  }

  override def updatePartitionInfo(
    executionAttemptID: org.apache.flink.runtime.executiongraph.ExecutionAttemptID,
    partitionInfo: org.apache.flink.runtime.executiongraph.PartitionInfo
  ): Boolean = true

  override def close(): Unit = MeteorPluginContext.shutdown()
}

// ================================
// ResultPartitionWriter — 使用 Flink Buffer 实现 in-memory PIPELINED 数据交换
// ================================

class MeteorResultPartitionWriter(
  ownerName: String,
  partitionId: IntermediateResultPartitionID,
  attemptId: org.apache.flink.runtime.executiongraph.ExecutionAttemptID,
  numSubpartitions: Int,
  shuffleDesc: Option[MeteorShuffleDescriptor]
) extends ResultPartitionWriter {

  import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog
  import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView.AvailabilityWithBacklog

  private val log = org.slf4j.LoggerFactory.getLogger(getClass)
  private val writerId = System.identityHashCode(this)
  // 每个 subpartition 一个已完成的 Buffer 队列
  private val subpartitionQueues: Array[ju.Queue[BufferAndBacklog]] =
    Array.fill(numSubpartitions)(new ConcurrentLinkedQueue[BufferAndBacklog]())
  // 当前写入的 BufferBuilder（每个 subpartition）
  private val bufferBuilders: Array[BufferBuilder] = Array.ofDim(numSubpartitions)
  private val finished = new AtomicBoolean(false)
  private var allDataProcessedFuture: CompletableFuture[Void] = _
  @volatile private var views: java.util.List[(MeteorSubpartitionView, BufferAvailabilityListener)] = new java.util.ArrayList()
  private val BufferSize = 32768 // 32KB

  override def setup(): Unit = {
    log.info(s"[MeteorWriter] setup owner=$ownerName writerId=$writerId partitionId=$partitionId attemptId=$attemptId numSub=$numSubpartitions")
    allDataProcessedFuture = new CompletableFuture[Void]()
  }

  override def getPartitionId: ResultPartitionID =
    new ResultPartitionID(partitionId, attemptId)

  override def getNumberOfSubpartitions: Int = numSubpartitions
  override def getNumTargetKeyGroups: Int = 0
  override def setMaxOverdraftBuffersPerGate(max: Int): Unit = {}
  override def setMetricGroup(metricGroup: TaskIOMetricGroup): Unit = {}

  private def getOrCreateBuilder(subpartition: Int): BufferBuilder = {
    var builder = bufferBuilders(subpartition)
    if (builder == null) {
      val seg = MemorySegmentFactory.allocateUnpooledSegment(BufferSize)
      builder = new BufferBuilder(seg, FreeingBufferRecycler.INSTANCE)
      bufferBuilders(subpartition) = builder
    }
    builder
  }

  private def finishCurrentBuffer(subpartition: Int): Unit = {
    val builder = bufferBuilders(subpartition)
    if (builder == null) {
      log.info(s"[MeteorWriter] finishCurrentBuffer skip-null writerId=$writerId partitionId=$partitionId subpartition=$subpartition")
      return
    }

    val committed = builder.getCommittedBytes
    if (committed == 0) {
      bufferBuilders(subpartition) = null
      log.info(s"[MeteorWriter] finishCurrentBuffer skip-empty writerId=$writerId partitionId=$partitionId subpartition=$subpartition")
      return
    }

    builder.finish()
    val consumer = builder.createBufferConsumerFromBeginning()
    val buffer = consumer.build()
    subpartitionQueues(subpartition).offer(
      new BufferAndBacklog(buffer, 0, Buffer.DataType.DATA_BUFFER, subpartition))
    bufferBuilders(subpartition) = null
  }

  override def emitRecord(record: java.nio.ByteBuffer, targetSubpartition: Int): Unit = {
    if (finished.get()) return
    val builder = getOrCreateBuilder(targetSubpartition)
    val remaining = record.remaining()
    val capacity = builder.getMaxCapacity - builder.getCommittedBytes
    val toWrite = Math.min(remaining, capacity)
    val slice = record.slice()
    slice.limit(toWrite)
    builder.append(slice)
    builder.commit()
    record.position(record.position() + toWrite)
    // 每条记录写完后立即 flush，使数据对下游可用
    finishCurrentBuffer(targetSubpartition)
    notifyDataAvailable()
    if (record.hasRemaining) {
      emitRecord(record, targetSubpartition)
    }
  }

  override def broadcastRecord(record: java.nio.ByteBuffer): Unit = {
    for (i <- 0 until numSubpartitions) {
      emitRecord(record, i)
    }
  }

  override def broadcastEvent(event: AbstractEvent, isPriorityEvent: Boolean): Unit = {
    log.info(s"[MeteorWriter] broadcastEvent writerId=$writerId partitionId=$partitionId event=${event.getClass.getName} priority=$isPriorityEvent")
  }
  override def alignedBarrierTimeout(barrierId: Long): Unit = {
    log.info(s"[MeteorWriter] alignedBarrierTimeout writerId=$writerId partitionId=$partitionId barrierId=$barrierId")
  }
  override def abortCheckpoint(checkpointId: Long, cause: CheckpointException): Unit = {
    log.info(s"[MeteorWriter] abortCheckpoint writerId=$writerId partitionId=$partitionId checkpointId=$checkpointId cause=$cause")
  }
  override def notifyEndOfData(stopMode: StopMode): Unit = {
    log.info(s"[MeteorWriter] notifyEndOfData writerId=$writerId partitionId=$partitionId stopMode=$stopMode")
  }

  override def finish(): Unit = {
    if (!finished.compareAndSet(false, true)) return
    log.info(s"[MeteorWriter] finish writerId=$writerId partitionId=$partitionId views=${views.size()}")
    val beforeFlushQueues = (0 until numSubpartitions)
      .map(i => s"$i=${subpartitionQueues(i).size()}")
      .mkString(",")
    log.info(s"[MeteorWriter] finish before-flush writerId=$writerId partitionId=$partitionId numSubpartitions=$numSubpartitions queues=$beforeFlushQueues")

    // 刷新所有未完成的 buffer
    for (i <- 0 until numSubpartitions) finishCurrentBuffer(i)
    val afterFlushQueues = (0 until numSubpartitions)
      .map(i => s"$i=${subpartitionQueues(i).size()}")
      .mkString(",")
    log.info(s"[MeteorWriter] finish after-flush writerId=$writerId partitionId=$partitionId queues=$afterFlushQueues")
    notifyDataAvailable()

    if (views.size() > 0) {
      val queueSizes = (0 until numSubpartitions)
        .map(i => s"$i=${subpartitionQueues(i).size()}")
        .mkString(",")
      log.info(s"[MeteorWriter] finish local-exchange writerId=$writerId partitionId=$partitionId views=${views.size()} queues=$queueSizes")
      if (allDataProcessedFuture != null) allDataProcessedFuture.complete(null)
      return
    }

    // 如果 Master 不可用，直接标记完成
    if (!MeteorPluginContext.isInitialized || allDataProcessedFuture == null) {
      if (allDataProcessedFuture != null) allDataProcessedFuture.complete(null)
      return
    }

    // Master 可用：推送数据到 Worker
    val client = MeteorPluginContext.getClient
    val system = MeteorPluginContext.getSystem
    val latch = new CountDownLatch(numSubpartitions)
    val pending = new AtomicInteger(numSubpartitions)

    import org.apache.pekko.actor.typed.ActorRef
    for (i <- 0 until numSubpartitions) {
      val queue = subpartitionQueues(i)
      val baos = new java.io.ByteArrayOutputStream()
      var buf = queue.poll()
      while (buf != null) {
        baos.write(buf.buffer().getNioBufferReadable().array())
        buf.buffer().recycleBuffer()
        buf = queue.poll()
      }
      val data = baos.toByteArray
      if (data.isEmpty) {
        latch.countDown()
        if (pending.decrementAndGet() == 0) allDataProcessedFuture.complete(null)
      } else {
        val shuffleId = ShuffleId(s"${partitionId}-$i", 0)
        val responseProbe = org.apache.pekko.actor.typed.scaladsl.Behaviors.setup[PushDataResponse] { _ =>
          org.apache.pekko.actor.typed.scaladsl.Behaviors.receiveMessage { _ =>
            if (pending.decrementAndGet() == 0) allDataProcessedFuture.complete(null)
            latch.countDown()
            org.apache.pekko.actor.typed.scaladsl.Behaviors.stopped
          }
        }
        val probe = system.systemActorOf(
          responseProbe, s"finish-probe-${partitionId.hashCode.abs}-$i-${System.nanoTime()}")
        client.tell(
          ShuffleClient.Push(shuffleId, partitionIndex = i, attemptNumber = 0,
            data = data, replyTo = probe.asInstanceOf[ActorRef[PushDataResponse]]))
      }
    }

    if (pending.get() > 0) {
      val allDone = latch.await(30, TimeUnit.SECONDS)
      if (!allDone) {
        allDataProcessedFuture.completeExceptionally(
          new RuntimeException(s"Push timeout for partition $partitionId"))
      }
    }
  }

  private def notifyDataAvailable(): Unit = {
    val snap = views
    snap.forEach { case (view, listener) =>
      if (listener != null && view != null) {
        listener.notifyDataAvailable(view)
      }
    }
  }

  override def getAllDataProcessedFuture: CompletableFuture[Void] = {
    if (allDataProcessedFuture == null) {
      allDataProcessedFuture = CompletableFuture.completedFuture(null)
    }
    allDataProcessedFuture
  }

  override def getAvailableFuture: CompletableFuture[_] = {
    org.apache.flink.runtime.io.AvailabilityProvider.AVAILABLE
  }

  override def createSubpartitionView(
    indexSet: org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet,
    listener: BufferAvailabilityListener
  ): ResultSubpartitionView = {
    log.info(s"[MeteorWriter] createSubpartitionView writerId=$writerId partitionId=$partitionId attemptId=$attemptId indexSet=$indexSet")
    val subIdx = indexSet.getStartIndex
    val view = new MeteorSubpartitionView(this, listener, subIdx)
    views.add((view, listener))
    view
  }

  override def flushAll(): Unit = {
    log.info(s"[MeteorWriter] flushAll writerId=$writerId partitionId=$partitionId")
    for (i <- 0 until numSubpartitions) finishCurrentBuffer(i)
    notifyDataAvailable()
  }
  override def flush(subpartitionIndex: Int): Unit = {
    log.info(s"[MeteorWriter] flush writerId=$writerId partitionId=$partitionId subpartition=$subpartitionIndex")
    finishCurrentBuffer(subpartitionIndex)
    notifyDataAvailable()
  }
  override def fail(cause: Throwable): Unit = {
    log.info(s"[MeteorWriter] fail writerId=$writerId partitionId=$partitionId cause=$cause")
    if (allDataProcessedFuture != null) {
      allDataProcessedFuture.completeExceptionally(cause)
    }
  }
  override def isFinished: Boolean = {
    val done = finished.get()
    log.info(s"[MeteorWriter] isFinished writerId=$writerId partitionId=$partitionId result=$done")
    done
  }
  override def release(cause: Throwable): Unit = {
    log.info(s"[MeteorWriter] release writerId=$writerId partitionId=$partitionId cause=$cause")
  }
  override def isReleased: Boolean = false
  override def close(): Unit = {
    val queueSizes = (0 until numSubpartitions)
      .map(i => s"$i=${subpartitionQueues(i).size()}")
      .mkString(",")
    log.info(s"[MeteorWriter] close writerId=$writerId partitionId=$partitionId queues=$queueSizes")
  }

  // ================================
  // 内部 ResultSubpartitionView — 从 subpartition 队列拉取 Buffer
  // ================================

  class MeteorSubpartitionView(
    parent: MeteorResultPartitionWriter,
    listener: BufferAvailabilityListener,
    subpartitionIndex: Int
  ) extends ResultSubpartitionView {
    private val released = new AtomicBoolean(false)

    override def getNextBuffer(): BufferAndBacklog = {
      if (subpartitionIndex < parent.subpartitionQueues.length) {
        parent.subpartitionQueues(subpartitionIndex).poll()
      } else {
        null
      }
    }

    override def notifyDataAvailable(): Unit = {
      listener.notifyDataAvailable(this)
    }

    override def releaseAllResources(): Unit = {
      if (released.compareAndSet(false, true)) {
        val queue = parent.subpartitionQueues(subpartitionIndex)
        var buf = queue.poll()
        while (buf != null) {
          buf.buffer().recycleBuffer()
          buf = queue.poll()
        }
      }
    }

    override def isReleased: Boolean = released.get()
    override def resumeConsumption(): Unit = {}
    override def acknowledgeAllDataProcessed(): Unit = {}
    override def getFailureCause: Throwable = null

    override def getAvailabilityAndBacklog(isCreditAvailable: Boolean): AvailabilityWithBacklog = {
      val queued = parent.subpartitionQueues(subpartitionIndex).size()
      if (queued > 0) {
        new AvailabilityWithBacklog(true, queued)
      } else if (parent.finished.get()) {
        new AvailabilityWithBacklog(false, 0)
      } else {
        new AvailabilityWithBacklog(false, 0)
      }
    }

    override def unsynchronizedGetNumberOfQueuedBuffers(): Int = {
      parent.subpartitionQueues(subpartitionIndex).size()
    }

    override def getNumberOfQueuedBuffers(): Int = unsynchronizedGetNumberOfQueuedBuffers()
    override def notifyNewBufferSize(newBufferSize: Int): Unit = {}
    override def peekNextBufferSubpartitionId(): Int = {
      if (!parent.subpartitionQueues(subpartitionIndex).isEmpty) subpartitionIndex
      else -1
    }
  }
}

// ================================
// IndexedInputGate — 支持本地 in-memory 和远程拉取两种模式
// ================================

class MeteorIndexedInputGate(
  ownerContext: ShuffleIOOwnerContext,
  shuffleDescs: Seq[ShuffleDescriptor],
  consumedSubpartitionIndex: Int = 0
) extends IndexedInputGate {

  private val resultQueue = new ConcurrentLinkedQueue[BufferOrEvent]()
  private val finished = new AtomicBoolean(false)
  private var stateConsumedFuture: CompletableFuture[Void] = _
  private var endOfDataStatus: org.apache.flink.runtime.io.PullingAsyncDataInput.EndOfDataStatus =
    org.apache.flink.runtime.io.PullingAsyncDataInput.EndOfDataStatus.NOT_END_OF_DATA
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  // 本地 subpartition views（从同 TM 的 MeteorResultPartitionWriter 读取）
  private var localViews: Seq[ResultSubpartitionView] = Seq.empty
  private var channelInfos: Seq[InputChannelInfo] = Seq.empty
  private var channels: Seq[InputChannel] = Seq.empty
  private var numChannels: Int = 0
  private val availabilityLock = new Object
  @volatile private var availableFuture: CompletableFuture[Void] = new CompletableFuture[Void]()

  private def markAvailable(reason: String): Unit = {
    availabilityLock.synchronized {
      if (!availableFuture.isDone) {
        log.info(s"[MeteorGate] markAvailable reason=$reason queue=${resultQueue.size()} finished=${finished.get()} channels=$numChannels")
        availableFuture.complete(null)
      } else {
        log.info(s"[MeteorGate] markAvailable already-done reason=$reason queue=${resultQueue.size()} finished=${finished.get()} channels=$numChannels")
      }
    }
  }

  private def resetAvailabilityIfNeeded(reason: String): Unit = {
    availabilityLock.synchronized {
      if (resultQueue.isEmpty && !finished.get() && availableFuture.isDone) {
        availableFuture = new CompletableFuture[Void]()
      } else {
      }
    }
  }

  /** 创建最小的 SingleInputGate，仅用于 InputChannel 构造函数 */
  private def createDummySingleInputGate(): org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate = {
    val gate = new org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate(
      "meteor-dummy",
      0, // gateIndex
      new org.apache.flink.runtime.jobgraph.IntermediateDataSetID,
      org.apache.flink.runtime.io.network.partition.ResultPartitionType.PIPELINED_BOUNDED,
      1, // numberOfInputChannels
      new org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider {
        override def requestPartitionProducerState(
          dataSetId: org.apache.flink.runtime.jobgraph.IntermediateDataSetID,
          partitionId: org.apache.flink.runtime.io.network.partition.ResultPartitionID,
          consumer: java.util.function.Consumer[_ >: org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider.ResponseHandle]
        ): Unit = {}
      },
      new org.apache.flink.util.function.SupplierWithException[org.apache.flink.runtime.io.network.buffer.BufferPool, java.io.IOException] {
        override def get(): org.apache.flink.runtime.io.network.buffer.BufferPool = null
      },
      null, // bufferDecompressor
      new org.apache.flink.core.memory.MemorySegmentProvider {
        override def requestUnpooledMemorySegments(
          numSegments: Int
        ): java.util.Collection[org.apache.flink.core.memory.MemorySegment] =
          java.util.Collections.emptyList()
        override def recycleUnpooledMemorySegments(
          segments: java.util.Collection[org.apache.flink.core.memory.MemorySegment]
        ): Unit = {}
      },
      0, // segmentSize
      new org.apache.flink.runtime.throughput.ThroughputCalculator(
        org.apache.flink.util.clock.SystemClock.getInstance()),
      null // bufferDebloater
    )
    gate
  }

  /** 最小化 InputChannel 实现，只返回 subpartition index set */
  private class MeteorInputChannel(
    gate: org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate,
    channelIdx: Int,
    val _indexSet: org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet
  ) extends InputChannel(
    gate,
    channelIdx, // channelIndex — 每个 channel 唯一
    new org.apache.flink.runtime.io.network.partition.ResultPartitionID(), // partitionId
    _indexSet,
    2, // initialCredit (must be <= transportBufferPoolSize=2)
    2, // transportBufferPoolSize
    new org.apache.flink.metrics.SimpleCounter(), // numBytesIn
    new org.apache.flink.metrics.SimpleCounter() // numBuffersIn
  ) {
    override def getConsumedSubpartitionIndexSet: org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet = _indexSet
    override def acknowledgeAllRecordsProcessed(): Unit = {}
    override def getNextBuffer(): java.util.Optional[org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability] =
      java.util.Optional.empty()
    override def resumeConsumption(): Unit = {}
    def getBuffersInUseCount(): Int = 0
    def isReleased(): Boolean = false
    def peekNextBufferSubpartitionIdInternal(): Int = 0
    def sendTaskEvent(event: org.apache.flink.runtime.event.TaskEvent): Unit = {}
    def announceBufferSize(newBufferSize: Int): Unit = {}
    def requestSubpartitions(): Unit = {}
    def releaseAllResources(): Unit = {}
    def getConsumedPartitionType(): org.apache.flink.runtime.io.network.partition.ResultPartitionType =
      org.apache.flink.runtime.io.network.partition.ResultPartitionType.PIPELINED_BOUNDED
    def getLastReceivedBufferId(): Long = 0L
    def notifyRequiredSegmentId(requiredSegmentId: Int): Unit = {}
    def getCurrentBacklog(): Int = 0
  }

  override def getGateIndex: Int = 0
  override def getNumberOfInputChannels: Int = numChannels
  override def getUnfinishedChannels: ju.List[InputChannelInfo] =
    channelInfos.asJava
  override def triggerDebloating(): Unit = {}
  override def getConsumedPartitionType: org.apache.flink.runtime.io.network.partition.ResultPartitionType =
    org.apache.flink.runtime.io.network.partition.ResultPartitionType.PIPELINED_BOUNDED
  override def resumeConsumption(info: InputChannelInfo): Unit = {}

  override def setup(): Unit = {
    log.info(s"[MeteorGate] setup descriptors=${shuffleDescs.size}")
    stateConsumedFuture = new CompletableFuture[Void]()
    stateConsumedFuture.complete(null)

    // 创建 dummy gate 供 InputChannel 构造用
    val dummyGate = createDummySingleInputGate()
    // 尝试连接本地 partition views
    val viewsBuilder = Seq.newBuilder[ResultSubpartitionView]
    val infoBuilder = Seq.newBuilder[InputChannelInfo]
    val chanBuilder = Seq.newBuilder[InputChannel]

    shuffleDescs.foreach {
      case md: MeteorShuffleDescriptor =>
        log.info(s"[MeteorGate] descriptor shuffleId=${md.shuffleId} resultPartitionId=${md.resultPartitionId} numPartitions=${md.numPartitions} locations=${md.locations.size}")
        md.resultPartitionId match {
          case Some(rpid) =>
            val writer = MeteorPluginContext.getLocalPartition(rpid)
            if (writer != null) {
              // 只读 consumedSubpartitionIndex 对应的 subpartition
              val actualNumSubs = writer.getNumberOfSubpartitions
              val subIdx = math.min(consumedSubpartitionIndex, actualNumSubs - 1)
              log.info(s"[MeteorGate] writer has $actualNumSubs subpartitions, consuming subpartition=$subIdx")
              val channelIdx = numChannels
              val indexSet = new org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet(subIdx)
              val channelInfo = new InputChannelInfo(0, channelIdx)
              log.info(s"[MeteorGate] creating view for rpid=$rpid subpartition=$subIdx channel=$channelIdx")
              val view = writer.createSubpartitionView(
                indexSet,
                new BufferAvailabilityListener {
                  override def notifyDataAvailable(view: ResultSubpartitionView): Unit = {
                    drainView(view, channelInfo)
                  }
                }
              )
              drainView(view, channelInfo)
              infoBuilder += channelInfo
              chanBuilder += new MeteorInputChannel(dummyGate, channelIdx, indexSet)
              viewsBuilder += view
              numChannels += 1
            } else {
              log.warn(s"[MeteorGate] local writer not found for $rpid")
            }
          case None =>
            log.warn(s"[MeteorGate] no resultPartitionId in descriptor, cannot connect locally")
        }
      case other =>
        log.info(s"[MeteorGate] non-meteor descriptor class=${other.getClass.getName}")
    }
    if (!resultQueue.isEmpty) markAvailable("setup")
    localViews = viewsBuilder.result()
    channelInfos = infoBuilder.result()
    channels = chanBuilder.result()
    log.info(s"[MeteorGate] setup complete channels=$numChannels localViews=${localViews.size} channelInfos=$channelInfos queue=${resultQueue.size()}")
  }

  /** 从 ResultSubpartitionView 拉取数据到 resultQueue */
  private def drainView(view: ResultSubpartitionView, channelInfo: InputChannelInfo): Unit = {
    var drained = 0
    var buf = view.getNextBuffer()
    while (buf != null) {
      resultQueue.add(new BufferOrEvent(buf.buffer(), channelInfo))
      drained += 1
      buf = view.getNextBuffer()
    }
    if (drained > 0) markAvailable("drainView")
  }

  override def requestPartitions(): Unit = {
    log.info(s"[MeteorGate] requestPartitions views=${localViews.size} channelInfos=${channelInfos.size} queue=${resultQueue.size()} finished=${finished.get()}")
    if (localViews.nonEmpty) {
      for (i <- localViews.indices) {
        drainView(localViews(i), channelInfos(i))
      }
    }
    if (!resultQueue.isEmpty) markAvailable("requestPartitions")
  }

  override def pollNext: java.util.Optional[BufferOrEvent] = {
    val next = resultQueue.poll()
    if (next != null) {
      if (resultQueue.isEmpty) resetAvailabilityIfNeeded("pollNext")
      java.util.Optional.of(next)
    } else {
      resetAvailabilityIfNeeded("pollNext-empty")
      java.util.Optional.empty()
    }
  }

  override def getNext: java.util.Optional[BufferOrEvent] = {
    log.info(s"[MeteorGate] getNext enter queue=${resultQueue.size()} finished=${finished.get()}")
    var next: BufferOrEvent = null
    var loops = 0
    while (next == null && !finished.get()) {
      for (i <- localViews.indices) {
        drainView(localViews(i), channelInfos(i))
      }
      next = resultQueue.poll()
      if (next == null) {
        loops += 1
        if (loops % 100 == 0) {
          log.info(s"[MeteorGate] getNext waiting loops=$loops queue=${resultQueue.size()} finished=${finished.get()}")
        }
        Thread.sleep(10)
      }
    }
    if (next != null) {
      log.info(s"[MeteorGate] getNext hit channelInfo=${next.getChannelInfo} queueAfter=${resultQueue.size()}")
      java.util.Optional.of(next)
    } else {
      log.info(s"[MeteorGate] getNext empty finished=${finished.get()} queue=${resultQueue.size()}")
      java.util.Optional.empty()
    }
  }

  override def getAvailableFuture: CompletableFuture[_] = {
    if (!resultQueue.isEmpty) {
      log.info(s"[MeteorGate] getAvailableFuture AVAILABLE queue=${resultQueue.size()} finished=${finished.get()}")
      org.apache.flink.runtime.io.AvailabilityProvider.AVAILABLE
    } else if (finished.get()) {
      log.info(s"[MeteorGate] getAvailableFuture AVAILABLE finished queue=${resultQueue.size()}")
      org.apache.flink.runtime.io.AvailabilityProvider.AVAILABLE
    } else {
      availableFuture
    }
  }

  override def getChannel(index: Int): InputChannel = channels.lift(index).orNull

  override def sendTaskEvent(event: org.apache.flink.runtime.event.TaskEvent): Unit = {}
  override def acknowledgeAllRecordsProcessed(info: InputChannelInfo): Unit = {}
  override def finishReadRecoveredState(): Unit = {}

  override def getStateConsumedFuture: CompletableFuture[Void] = {
    if (stateConsumedFuture == null) stateConsumedFuture = CompletableFuture.completedFuture(null)
    stateConsumedFuture
  }

  override def hasReceivedEndOfData: org.apache.flink.runtime.io.PullingAsyncDataInput.EndOfDataStatus =
    endOfDataStatus

  override def close(): Unit = {
    log.info(s"[MeteorGate] close localViews=${localViews.size} queue=${resultQueue.size()}")
    finished.set(true)
    markAvailable("close")
    if (stateConsumedFuture != null) stateConsumedFuture.complete(null)
    localViews.foreach(_.releaseAllResources())
  }

  override def isFinished: Boolean = {
    val done = finished.get() && resultQueue.isEmpty
    done
  }
}

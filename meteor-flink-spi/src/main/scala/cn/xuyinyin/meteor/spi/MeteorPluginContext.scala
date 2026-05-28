package cn.xuyinyin.meteor.spi

import org.apache.pekko.actor.typed.{ActorSystem, ActorRef}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.slf4j.LoggerFactory
import org.apache.flink.runtime.io.network.partition.ResultPartitionID
import cn.xuyinyin.meteor.client.{ShuffleClient, LifecycleManager}
import cn.xuyinyin.meteor.common.{Protocol, MasterActorCommand}
import Protocol._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}

/**
 * Flink 插件上下文 — 管理 Pekko ActorSystem、LifecycleManager、ShuffleClient 生命周期
 *
 * 在 Flink JM/TM 进程内单例。
 * - JM 侧：用 LifecycleManager 管理 shuffle 注册/提交/注销
 * - TM 侧：用 ShuffleClient 做 PushData/FetchData
 *
 * 通过 Pekko remoting 连接远程 Meteor Master。
 */
object MeteorPluginContext {

  @volatile private var system: ActorSystem[Void] = _
  @volatile private var clientRef: ActorRef[ShuffleClient.Command] = _
  @volatile private var lifecycleManagerRef: ActorRef[LifecycleManager.Command] = _
  @volatile private var masterActorRef: ActorRef[MasterActorCommand] = _

  // 本地 in-memory 数据交换：ResultPartitionID → MeteorResultPartitionWriter
  private val localPartitions = new java.util.concurrent.ConcurrentHashMap[ResultPartitionID, MeteorResultPartitionWriter]()
  private val log = LoggerFactory.getLogger(getClass)

  def registerLocalPartition(id: ResultPartitionID, writer: MeteorResultPartitionWriter): Unit = {
    log.info(s"[MeteorPluginContext] registerLocalPartition id=$id writerId=${System.identityHashCode(writer)}")
    localPartitions.put(id, writer)
  }

  def getLocalPartition(id: ResultPartitionID): MeteorResultPartitionWriter = {
    val writer = localPartitions.get(id)
    log.info(s"[MeteorPluginContext] getLocalPartition id=$id writerId=${if (writer == null) "null" else System.identityHashCode(writer).toString}")
    writer
  }

  def removeLocalPartition(id: ResultPartitionID): Unit = {
    log.info(s"[MeteorPluginContext] removeLocalPartition id=$id")
    localPartitions.remove(id)
  }

  /**
   * 初始化插件上下文
   *
   * 如果 Master 不可用，插件仍然初始化（ActorSystem 创建），
   * 只是 masterActorRef/lifecycleManagerRef/clientRef 为空。
   * 后续操作会通过 getXxx 抛出 IllegalStateException。
   */
  def init(masterHost: String, masterPort: Int, replicateEnabled: Boolean = false): Unit = synchronized {
    if (system == null) {
      system = ActorSystem(
        Behaviors.setup[Void] { ctx =>
          import scala.concurrent.ExecutionContext.Implicits.global

          // 尝试连接远程 Master（非阻塞，失败不崩溃）
          Try {
            val systemName = "meteor-shuffle"
            val masterPath = s"pekko://$systemName@$masterHost:$masterPort/user/master"
            log.info(s"Connecting to Meteor Master at $masterPath ...")
            val masterFuture = ctx.system.classicSystem.actorSelection(masterPath).resolveOne(15.seconds)
            val masterRef = Await.result(masterFuture, 15.seconds)
            masterActorRef = masterRef.asInstanceOf[ActorRef[MasterActorCommand]]
            log.info(s"Connected to Meteor Master at $masterPath")
          } match {
            case Success(_) =>
              // 创建 LifecycleManager（JM 侧：shuffle 生命周期管理）
              val lm = ctx.spawn(
                LifecycleManager(masterActorRef),
                "meteor-lifecycle-manager"
              )
              lifecycleManagerRef = lm

              // 创建 ShuffleClient（TM 侧：数据推送/拉取）
              val client = ctx.spawn(
                ShuffleClient(masterActorRef, replicateEnabled),
                "meteor-shuffle-client"
              )
              clientRef = client

              ctx.watch(lm)
              ctx.watch(client)

            case Failure(ex) =>
              log.warn(s"Meteor Master not available ($masterHost:$masterPort), running without master: ${ex.getMessage}")
          }

          Behaviors.receiveSignal {
            case (_, org.apache.pekko.actor.typed.Terminated(_)) =>
              Behaviors.stopped
          }
        },
        "meteor-flink-plugin"
      )
      Thread.sleep(500) // 等待 actor 系统就绪
    }
  }

  def getClient: ActorRef[ShuffleClient.Command] = {
    if (clientRef == null) throw new IllegalStateException("MeteorPluginContext not initialized")
    clientRef
  }

  def getLifecycleManager: ActorRef[LifecycleManager.Command] = {
    if (lifecycleManagerRef == null) throw new IllegalStateException("MeteorPluginContext not initialized")
    lifecycleManagerRef
  }

  def getMasterRef: ActorRef[MasterActorCommand] = {
    if (masterActorRef == null) throw new IllegalStateException("MeteorPluginContext not initialized")
    masterActorRef
  }

  def isInitialized: Boolean = masterActorRef != null

  def getSystem: ActorSystem[Void] = {
    if (system == null) throw new IllegalStateException("MeteorPluginContext not initialized")
    system
  }

  def shutdown(): Unit = synchronized {
    if (system != null) {
      system.terminate()
      system = null
      clientRef = null
      lifecycleManagerRef = null
      masterActorRef = null
    }
  }
}

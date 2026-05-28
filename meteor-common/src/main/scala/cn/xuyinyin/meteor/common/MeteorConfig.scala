package cn.xuyinyin.meteor.common

import com.typesafe.config.{Config, ConfigFactory}

/**
 * Meteor 全局配置
 *
 * 对标 Celeborn 的 celeborn.* 配置体系，用 Typesafe Config 管理。
 * 用法：MeteorConfig.load() 或 MeteorConfig.from(config)
 */
object MeteorConfig {

  def load(): MeteorConfig = from(ConfigFactory.load())

  def from(config: Config): MeteorConfig = {
    val m = config.getConfig("meteor")
    MeteorConfig(
      role = m.getString("role"),  // master | worker
      masterEndpoints = m.getStringList("master.endpoints"),
      workerBindPort = m.getInt("worker.bind-port"),
      workerRpcPort = m.getInt("worker.rpc-port"),
      workerStorageDirs = m.getStringList("worker.storage.dirs"),
      workerFlusherThreads = m.getInt("worker.flusher.threads"),
      workerFlusherBufferSize = m.getBytes("worker.flusher.buffer-size"),
      clientPushReplicate = m.getBoolean("client.push.replicate"),
      clientPushMaxReqsInFlight = m.getInt("client.push.max-reqs-in-flight"),
      clientFetchMaxReqsInFlight = m.getInt("client.fetch.max-reqs-in-flight"),
      clientPushBufferSize = m.getBytes("client.push.buffer-size"),
      clientPushQueueCapacity = m.getInt("client.push.queue-capacity"),
      shuffleSplitThreshold = m.getBytes("shuffle.partition.split-threshold"),
      masterSlotAssignPolicy = m.getString("master.slot-assign-policy")
    )
  }
}

case class MeteorConfig(
  role: String,
  masterEndpoints: java.util.List[String],
  workerBindPort: Int,
  workerRpcPort: Int,
  workerStorageDirs: java.util.List[String],
  workerFlusherThreads: Int,
  workerFlusherBufferSize: Long,
  clientPushReplicate: Boolean,
  clientPushMaxReqsInFlight: Int,
  clientFetchMaxReqsInFlight: Int,
  clientPushBufferSize: Long,
  clientPushQueueCapacity: Int,
  shuffleSplitThreshold: Long,
  masterSlotAssignPolicy: String
)

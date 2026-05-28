package cn.xuyinyin.meteor.worker

import org.apache.pekko.actor.typed.{ActorRef, ActorSystem}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json._
import scala.concurrent.duration._

import cn.xuyinyin.meteor.common.Metrics

/**
 * Worker Admin HTTP API — 接入真实 Actor 状态
 *
 * 提供：
 *   GET /api/v1/health       — 健康检查
 *   GET /api/v1/partitions   — 当前 partition 列表（实时查询 WorkerActor）
 *   POST /api/v1/flush/:id   — 强制刷盘某个 partition
 *   GET /metrics             — Prometheus 指标
 */
object WorkerHttpServer extends SprayJsonSupport with DefaultJsonProtocol {

  implicit val partitionInfoFormat: RootJsonFormat[WorkerActor.PartitionInfo] = jsonFormat3(WorkerActor.PartitionInfo)
  implicit val partitionsResponseFormat: RootJsonFormat[WorkerActor.PartitionsQueryResponse] = jsonFormat1(WorkerActor.PartitionsQueryResponse)

  def start(host: String = "0.0.0.0", port: Int = 9091, workerRef: ActorRef[WorkerActor.Command])(implicit system: ActorSystem[_]): Unit = {

    implicit val timeout: org.apache.pekko.util.Timeout = org.apache.pekko.util.Timeout(5.seconds)

    val route: Route =
      pathPrefix("api" / "v1") {
        concat(
          path("health") {
            get {
              complete("""{"status":"ok","service":"meteor-worker"}""")
            }
          },
          path("partitions") {
            get {
              import org.apache.pekko.actor.typed.scaladsl.AskPattern._
              val future = workerRef.ask[WorkerActor.PartitionsQueryResponse](ref => WorkerActor.QueryPartitions(ref))
              onSuccess(future) { resp =>
                complete(resp)
              }
            }
          },
          path("flush" / Segment) { partitionId =>
            post {
              // TODO: 触发 partition flush
              complete(s"""{"status":"ok","partition":"$partitionId","note":"flush not yet implemented"}""")
            }
          }
        )
      } ~ Metrics.metricsRoute()

    Http().newServerAt(host, port).bind(route)
    system.log.info(s"Worker HTTP API listening on $host:$port")
  }
}

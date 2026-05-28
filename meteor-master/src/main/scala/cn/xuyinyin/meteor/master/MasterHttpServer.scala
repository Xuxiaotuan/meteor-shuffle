package cn.xuyinyin.meteor.master

import org.apache.pekko.actor.typed.{ActorRef, ActorSystem}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json._
import scala.concurrent.duration._

import cn.xuyinyin.meteor.common.Metrics

/**
 * Master Admin HTTP API — 接入真实 Actor 状态
 *
 * 提供：
 *   GET /api/v1/health     — 健康检查
 *   GET /api/v1/workers    — Worker 列表（实时查询 MasterActor）
 *   GET /api/v1/shuffles   — Shuffle 列表（实时查询 MasterActor）
 *   GET /metrics           — Prometheus 指标
 */
object MasterHttpServer extends SprayJsonSupport with DefaultJsonProtocol {

  // JSON 序列化
  implicit val workerStateInfoFormat: RootJsonFormat[MasterActor.WorkerStateInfo] = jsonFormat7(MasterActor.WorkerStateInfo)
  implicit val shuffleStateInfoFormat: RootJsonFormat[MasterActor.ShuffleStateInfo] = jsonFormat4(MasterActor.ShuffleStateInfo)
  implicit val workersResponseFormat: RootJsonFormat[MasterActor.WorkersQueryResponse] = jsonFormat1(MasterActor.WorkersQueryResponse)
  implicit val shufflesResponseFormat: RootJsonFormat[MasterActor.ShufflesQueryResponse] = jsonFormat1(MasterActor.ShufflesQueryResponse)

  def start(host: String = "0.0.0.0", port: Int = 9090, masterRef: ActorRef[MasterActor.Command])(implicit system: ActorSystem[_]): Unit = {

    implicit val timeout: org.apache.pekko.util.Timeout = org.apache.pekko.util.Timeout(5.seconds)

    val route: Route =
      pathPrefix("api" / "v1") {
        concat(
          path("health") {
            get {
              complete("""{"status":"ok","service":"meteor-master"}""")
            }
          },
          path("workers") {
            get {
              import org.apache.pekko.actor.typed.scaladsl.AskPattern._
              val future = masterRef.ask[MasterActor.WorkersQueryResponse](ref => MasterActor.QueryWorkers(ref))
              onSuccess(future) { resp =>
                complete(resp)
              }
            }
          },
          path("shuffles") {
            get {
              import org.apache.pekko.actor.typed.scaladsl.AskPattern._
              val future = masterRef.ask[MasterActor.ShufflesQueryResponse](ref => MasterActor.QueryShuffles(ref))
              onSuccess(future) { resp =>
                complete(resp)
              }
            }
          }
        )
      } ~ Metrics.metricsRoute()

    Http().newServerAt(host, port).bind(route)
    system.log.info(s"Master HTTP API listening on $host:$port")
  }
}

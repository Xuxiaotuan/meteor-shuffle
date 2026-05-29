package cn.xuyinyin.meteor.master

import org.apache.pekko.actor.typed.{ActorSystem, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.typed.{Cluster, ClusterSingleton, SingletonActor}
import com.typesafe.config.ConfigFactory
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Meteor Master 启动入口
 *
 * 支持单节点和 3 节点 HA 模式。
 * HA 模式使用 Pekko Cluster Singleton：当 Master 节点宕机，
 * Singleton 自动迁移到另一个健康节点，从 Event Sourcing Journal 恢复状态。
 *
 * 启动方式：
 *   节点 1: sbt "meteorMaster/run -Dpekko.remote.artery.canonical.port=2551"
 *   节点 2: sbt "meteorMaster/run -Dpekko.remote.artery.canonical.port=2552"
 *   节点 3: sbt "meteorMaster/run -Dpekko.remote.artery.canonical.port=2553"
 */
object MasterApp {

  sealed trait RootCmd

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load("master.conf")

    val system: ActorSystem[RootCmd] = ActorSystem[RootCmd](
      Behaviors.setup[RootCmd] { ctx =>
        val cluster = Cluster(ctx.system)
        ctx.log.info(s"Meteor Master starting at ${cluster.selfMember.address}")

        // 使用 Cluster Singleton 保证 Master Actor 唯一且可自动迁移
        // 当当前节点宕机，Singleton 自动在另一个节点启动新实例
        val singleton = ClusterSingleton(ctx.system)
        val singletonManager = singleton.init(
          SingletonActor(
            Behaviors.supervise(MasterActor()).onFailure(
              org.apache.pekko.actor.typed.SupervisorStrategy.restart
            ),
            "global-master"
          ).withStopMessage(MasterActor.GracefulShutdown)
        )

        // 启动 HTTP Admin API（接入 Singleton Master ref）
        MasterHttpServer.start(masterRef = singletonManager)(ctx.system)

        // JVM 优雅关闭 hook（不能用 ctx.log，shutdownHook 不在 Actor 线程里）
        val systemRef = ctx.system
        sys.addShutdownHook {
          println(s"[MasterApp] JVM shutdown hook triggered, terminating ActorSystem")
          systemRef.terminate()
        }

        Behaviors.same
      },
      "meteor-shuffle",
      config
    )

    // 保持 JVM 运行
    system.whenTerminated.foreach(_ => println("Master stopped"))(global)
    Await.result(system.whenTerminated, Duration.Inf)
  }
}

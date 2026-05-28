package cn.xuyinyin.meteor.master

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.typed.Cluster
import com.typesafe.config.ConfigFactory
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Meteor Master 启动入口
 */
object MasterApp {

  sealed trait RootCmd

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load("master.conf")

    val system: ActorSystem[RootCmd] = ActorSystem[RootCmd](
      Behaviors.setup[RootCmd] { ctx =>
        val cluster = Cluster(ctx.system)
        ctx.log.info(s"Meteor Master starting at ${cluster.selfMember.address}")

        val masterRef = ctx.spawn(MasterActor(), "master")
        ctx.watch(masterRef)

        // 启动 HTTP Admin API（接入真实 MasterActor 状态）
        MasterHttpServer.start(masterRef = masterRef)(ctx.system)

        // JVM 优雅关闭 hook
        sys.addShutdownHook {
          ctx.log.info("JVM shutdown hook triggered, sending GracefulShutdown to Master")
          masterRef ! MasterActor.GracefulShutdown
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

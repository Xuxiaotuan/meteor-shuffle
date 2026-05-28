package cn.xuyinyin.meteor.worker

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * Meteor Worker 启动入口
 */
object WorkerApp {

  sealed trait RootCmd

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load("worker.conf")
    val workerConfig = config.getConfig("meteor.worker")
    val storageDirs = workerConfig.getStringList("storage.dirs").asScala.toSeq
    val bindPort = workerConfig.getInt("bind-port")
    val dataPort = workerConfig.getInt("data-port")

    val system: ActorSystem[RootCmd] = ActorSystem[RootCmd](
      Behaviors.setup[RootCmd] { ctx =>
        ctx.log.info(s"Meteor Worker starting at ${ctx.system.address}")
        ctx.log.info(s"Storage dirs: ${storageDirs.mkString(", ")}")
        val workerRef = ctx.spawn(
          WorkerActor(
            storageDirs = storageDirs,
            host = "127.0.0.1",
            port = bindPort,
            dataPort = dataPort
          ),
          "worker"
        )

        // HTTP Admin API（接入真实 WorkerActor 状态）
        WorkerHttpServer.start(workerRef = workerRef)(ctx.system)

        // JVM 优雅关闭 hook
        sys.addShutdownHook {
          ctx.log.info("JVM shutdown hook triggered, sending GracefulShutdown to Worker")
          workerRef ! WorkerActor.GracefulShutdown
        }

        Behaviors.empty
      },
      "meteor-shuffle",
      config
    )

    Await.result(system.whenTerminated, Duration.Inf)
  }
}

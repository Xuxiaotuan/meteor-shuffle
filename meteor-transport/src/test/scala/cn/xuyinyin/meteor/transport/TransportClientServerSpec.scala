package cn.xuyinyin.meteor.transport

import java.net.ServerSocket
import java.nio.file.{Path, Paths}
import java.util.concurrent.ConcurrentHashMap
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

class TransportClientServerSpec extends AnyWordSpec with Matchers with ScalaFutures {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = Span(5, Seconds),
    interval = Span(50, Millis)
  )

  "TransportClient and TransportServer" should {

    "replicate and fetch data through the transport server" in {
      val port = freePort()
      val storage = new InMemoryStorageAdapter
      val server = new TransportServer("127.0.0.1", port, storage).start()
      val client = new TransportClient()
      val payload = "replica-data".getBytes("UTF-8")

      try {
        val body = TransportCodec.PushBody("app", 1, 2, 3, payload, 0L, 0)
        client.replicate("127.0.0.1", port, body).futureValue shouldBe true

        val fetched = client.fetch("127.0.0.1", port, TransportCodec.FetchBody("app", 1, 2, 3)).futureValue
        fetched.toSeq shouldEqual payload.toSeq
      } finally {
        client.close()
        server.close()
      }
    }

    "fail or time out when connecting to a missing server" in {
      val port = freePort()
      val client = new TransportClient()
      try {
        val body = TransportCodec.PushBody("app", 1, 2, 3, Array[Byte](1), 0L, 0)
        client.replicate("127.0.0.1", port, body, timeoutMs = 200).futureValue shouldBe false
      } finally {
        client.close()
      }
    }
  }

  private def freePort(): Int = {
    val socket = new ServerSocket(0)
    try socket.getLocalPort
    finally socket.close()
  }

  private class InMemoryStorageAdapter extends StorageAdapter {
    private val data = new ConcurrentHashMap[String, Array[Byte]]()
    private val complete = ConcurrentHashMap.newKeySet[String]()

    override def write(key: String, bytes: Array[Byte]): Unit = data.put(key, bytes)
    override def readComplete(key: String): Option[Array[Byte]] = {
      if (complete.contains(key)) Option(data.get(key)) else None
    }
    override def markComplete(key: String): Unit = complete.add(key)
    override def selectDir(shuffleId: String, partitionIndex: Int): Path = Paths.get(".")
  }
}

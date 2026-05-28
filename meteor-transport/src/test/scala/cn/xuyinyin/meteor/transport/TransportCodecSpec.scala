package cn.xuyinyin.meteor.transport

import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TransportCodecSpec extends AnyWordSpec with Matchers {

  "TransportCodec" should {

    "decode a frame after Netty length-field framing" in {
      val body = TransportCodec.encodeFetchBody(
        TransportCodec.FetchBody("app", 1, 2, 3)
      )
      val frame = TransportCodec.encodeFrame(TransportCodec.TypeFetchRequest, 42, body)
      val channel = new EmbeddedChannel(
        new LengthFieldBasedFrameDecoder(Int.MaxValue, 0, 4, -4, 0)
      )

      channel.writeInbound(frame)
      val decoded = channel.readInbound[io.netty.buffer.ByteBuf]()
      val result = TransportCodec.decodeFrame(decoded)

      result.isDefined shouldBe true
      val (msgType, reqId, decodedBody) = result.get
      msgType shouldEqual TransportCodec.TypeFetchRequest
      reqId shouldEqual 42
      TransportCodec.decodeFetchBody(decodedBody) shouldEqual TransportCodec.FetchBody("app", 1, 2, 3)
      decodedBody.release()
      decoded.release()
      channel.finishAndReleaseAll()
    }

    "round-trip push bodies" in {
      val payload = "payload".getBytes("UTF-8")
      val body = TransportCodec.PushBody("app", 7, 8, 9, payload, 123L, 0)
      val encoded = TransportCodec.encodePushBody(body)
      val decoded = TransportCodec.decodePushBody(io.netty.buffer.Unpooled.wrappedBuffer(encoded))

      decoded.appId shouldEqual body.appId
      decoded.shuffleIndex shouldEqual body.shuffleIndex
      decoded.partitionIndex shouldEqual body.partitionIndex
      decoded.attemptNumber shouldEqual body.attemptNumber
      decoded.data shouldEqual payload
      decoded.checksum shouldEqual body.checksum
      decoded.compression shouldEqual body.compression
    }

    "round-trip fetch responses" in {
      val payload = "data".getBytes("UTF-8")
      val respBytes = TransportCodec.encodeFetchResponse(success = true, payload, "")
      val decoded3 = TransportCodec.decodeFetchResponse(io.netty.buffer.Unpooled.wrappedBuffer(respBytes))

      decoded3._1 shouldBe true
      decoded3._2 shouldEqual payload
      decoded3._3 shouldEqual ""
    }
  }
}

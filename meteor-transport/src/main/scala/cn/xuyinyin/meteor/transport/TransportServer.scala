package cn.xuyinyin.meteor.transport

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.buffer.ByteBuf
import cn.xuyinyin.meteor.common.Codec
import org.slf4j.LoggerFactory

/**
 * Netty Transport Server — 直连数据面入口。
 *
 * 接收其他 Worker 的 Push/Fetch/Replicate 请求，通过 StorageAdapter 操作本地存储。
 * 不经过 Pekko Actor，消除序列化开销。
 */
class TransportServer(
  host: String,
  port: Int,
  storage: StorageAdapter
) extends AutoCloseable {

  private val log  = LoggerFactory.getLogger(getClass)
  private val boss = new NioEventLoopGroup(1)
  private val work = new NioEventLoopGroup()

  @volatile private var channel: Channel = _

  def start(): TransportServer = {
    val b = new ServerBootstrap()
    b.group(boss, work)
      .channel(classOf[NioServerSocketChannel])
      .childHandler(new ChannelInitializer[Channel] {
        override def initChannel(ch: Channel): Unit = {
          val pipe = ch.pipeline()
          pipe.addLast("framer", new LengthFieldBasedFrameDecoder(Int.MaxValue, 0, 4, -4, 0))
          pipe.addLast("handler", new DataHandler(storage))
        }
      })

    val future = b.bind(host, port).sync()
    channel = future.channel()
    log.info(s"TransportServer started on $host:$port")
    this
  }

  override def close(): Unit = {
    if (channel != null) channel.close().sync()
    boss.shutdownGracefully()
    work.shutdownGracefully()
    log.info(s"TransportServer on $host:$port stopped")
  }
}

/**
 * Netty ChannelHandler — 处理 Push/Fetch/Replicate 请求。
 */
class DataHandler(storage: StorageAdapter) extends ChannelInboundHandlerAdapter {

  private val log = LoggerFactory.getLogger(getClass)

  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
    val buf = msg.asInstanceOf[ByteBuf]
    try {
      TransportCodec.decodeFrame(buf) match {
        case Some((msgType, reqId, body)) =>
          try {
            msgType match {
              case TransportCodec.TypePushRequest  => handlePush(ctx, reqId, body)
              case TransportCodec.TypeFetchRequest => handleFetch(ctx, reqId, body)
              case TransportCodec.TypeReplicateReq => handleReplicate(ctx, reqId, body)
              case _ => log.warn(s"Unknown type: $msgType"); ctx.close()
            }
          } finally { body.release() }
        case None => log.warn("Incomplete frame")
      }
    } finally { buf.release() }
  }

  private def handlePush(ctx: ChannelHandlerContext, reqId: Int, body: ByteBuf): Unit = {
    val pb = TransportCodec.decodePushBody(body)
    val key = writerKey(pb.appId, pb.shuffleIndex, pb.partitionIndex, pb.attemptNumber)

    try {
      if (pb.checksum != 0) {
        val actual = Codec.crc32(pb.data).toLong & 0xFFFFFFFFL
        if (actual != pb.checksum) {
          writePushResp(ctx, reqId, success = false, s"Checksum mismatch: expected=${pb.checksum}, actual=$actual")
          return
        }
      }

      val data = if (pb.compression != 0) Codec.decompressByByte(pb.data, pb.compression) else pb.data
      storage.write(key, data)
      log.debug(s"Transport: pushed ${data.length}B to $key")
      writePushResp(ctx, reqId, success = true)
    } catch {
      case ex: Exception =>
        log.error(s"Push failed: $key", ex)
        writePushResp(ctx, reqId, success = false, ex.getMessage)
    }
  }

  private def handleFetch(ctx: ChannelHandlerContext, reqId: Int, body: ByteBuf): Unit = {
    val fb = TransportCodec.decodeFetchBody(body)
    val key = writerKey(fb.appId, fb.shuffleIndex, fb.partitionIndex, fb.attemptNumber)

    storage.readComplete(key).orElse(storage.readComplete(replicaKey(fb.appId, fb.shuffleIndex, fb.partitionIndex, fb.attemptNumber))) match {
      case Some(data) =>
        val resp = TransportCodec.encodeFrame(
          TransportCodec.TypeFetchResponse, reqId,
          TransportCodec.encodeFetchResponse(success = true, data))
        ctx.writeAndFlush(resp)
        log.debug(s"Transport: fetched ${data.length}B from $key")
      case None =>
        val resp = TransportCodec.encodeFrame(
          TransportCodec.TypeFetchResponse, reqId,
          TransportCodec.encodeFetchResponse(success = false, Array.emptyByteArray, "Partition not found or not complete"))
        ctx.writeAndFlush(resp)
    }
  }

  private def handleReplicate(ctx: ChannelHandlerContext, reqId: Int, body: ByteBuf): Unit = {
    val rb = TransportCodec.decodeReplicateBody(body)
    val key = replicaKey(rb.appId, rb.shuffleIndex, rb.partitionIndex, rb.attemptNumber)

    try {
      storage.write(key, rb.data)
      storage.markComplete(key)
      val resp = TransportCodec.encodeFrame(
        TransportCodec.TypeReplicateRsp, reqId,
        TransportCodec.encodeReplicateResponse(success = true))
      ctx.writeAndFlush(resp)
      log.debug(s"Transport: replicated ${rb.data.length}B to $key")
    } catch {
      case ex: Exception =>
        log.error(s"Replicate failed: $key", ex)
        val resp = TransportCodec.encodeFrame(
          TransportCodec.TypeReplicateRsp, reqId,
          TransportCodec.encodeReplicateResponse(success = false, ex.getMessage))
        ctx.writeAndFlush(resp)
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    log.error("Transport channel exception", cause)
    ctx.close()
  }

  private def writePushResp(ctx: ChannelHandlerContext, reqId: Int, success: Boolean, err: String = ""): Unit = {
    val resp = TransportCodec.encodeFrame(
      TransportCodec.TypePushResponse, reqId,
      TransportCodec.encodePushResponse(success, err))
    ctx.writeAndFlush(resp)
  }

  private def writerKey(appId: String, shuffleIdx: Int, partIdx: Int, attempt: Int): String =
    s"$appId:$shuffleIdx:$partIdx:$attempt"

  private def replicaKey(appId: String, shuffleIdx: Int, partIdx: Int, attempt: Int): String =
    s"repl:$appId:$shuffleIdx:$partIdx:$attempt"
}

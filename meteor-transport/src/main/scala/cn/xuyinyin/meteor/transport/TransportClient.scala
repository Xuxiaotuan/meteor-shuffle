package cn.xuyinyin.meteor.transport

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import org.slf4j.LoggerFactory
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

/**
 * Netty Transport Client — 连接池 + 异步请求/响应。
 *
 * 用于 Worker 向其他 Worker 发送 Push/Replicate/Fetch 数据面请求。
 * 连接缓存：ConcurrentHashMap[(host,port) → Channel]，复用 TCP 连接。
 */
class TransportClient(ioGroup: NioEventLoopGroup = new NioEventLoopGroup(2)) extends AutoCloseable {

  private val log = LoggerFactory.getLogger(getClass)
  private val channels = new ConcurrentHashMap[String, Channel]()
  private val pendingReqs = new ConcurrentHashMap[Int, Promise[Any]]()
  private val reqIdGen = new AtomicInteger(1)

  /** 获取或创建到指定 Worker 的 Channel */
  private def getChannel(host: String, port: Int): Future[Channel] = {
    val key = s"$host:$port"
    Option(channels.get(key)).filter(_.isActive) match {
      case Some(ch) => Future.successful(ch)
      case None =>
        val promise = Promise[Channel]()
        val b = new Bootstrap()
        b.group(ioGroup)
          .channel(classOf[NioSocketChannel])
          .handler(new ChannelInitializer[Channel] {
            override def initChannel(ch: Channel): Unit = {
              val pipe = ch.pipeline()
              pipe.addLast("framer", new LengthFieldBasedFrameDecoder(Int.MaxValue, 0, 4, -4, 0))
              pipe.addLast("clientHandler", new ClientResponseHandler(pendingReqs))
            }
          })

        b.connect(host, port).addListener((f: ChannelFuture) => {
          if (f.isSuccess) {
            val ch = f.channel()
            channels.put(key, ch)
            ch.closeFuture().addListener((_: ChannelFuture) => channels.remove(key))
            log.info(s"TransportClient connected to $key")
            promise.success(ch)
          } else {
            log.error(s"TransportClient failed to connect to $key", f.cause())
            promise.failure(f.cause())
          }
        })
        promise.future
    }
  }

  /** 发送 PushData 请求，返回 Future[Boolean] */
  def push(host: String, port: Int, body: TransportCodec.PushBody): Future[Boolean] = {
    val reqId = reqIdGen.incrementAndGet()
    val frame = TransportCodec.encodeFrame(TransportCodec.TypePushRequest, reqId,
      TransportCodec.encodePushBody(body))
    sendAndReceive(host, port, reqId, frame).map {
      case (success: Boolean, _: String) => success
      case _ => false
    }
  }

  /** 发送 FetchData 请求，返回 Future[Array[Byte]] */
  def fetch(host: String, port: Int, body: TransportCodec.FetchBody): Future[Array[Byte]] = {
    val reqId = reqIdGen.incrementAndGet()
    val frame = TransportCodec.encodeFrame(TransportCodec.TypeFetchRequest, reqId,
      TransportCodec.encodeFetchBody(body))
    sendAndReceive(host, port, reqId, frame).map {
      case (_: Boolean, data: Array[Byte], _: String) => data
      case _ => Array.emptyByteArray
    }
  }

  /** 发送 ReplicateData 请求，返回 Future[Boolean] */
  def replicate(host: String, port: Int, body: TransportCodec.PushBody): Future[Boolean] = {
    val reqId = reqIdGen.incrementAndGet()
    val frame = TransportCodec.encodeFrame(TransportCodec.TypeReplicateReq, reqId,
      TransportCodec.encodeReplicateBody(body))
    sendAndReceive(host, port, reqId, frame).map {
      case (success: Boolean, _: String) => success
      case _ => false
    }
  }

  private def sendAndReceive(host: String, port: Int, reqId: Int, frame: ByteBuf): Future[Any] = {
    val promise = Promise[Any]()
    pendingReqs.put(reqId, promise)
    getChannel(host, port).onComplete {
      case Success(ch) =>
        ch.writeAndFlush(frame).addListener((f: ChannelFuture) => {
          if (!f.isSuccess) {
            pendingReqs.remove(reqId)
            promise.tryFailure(f.cause())
          }
        })
      case Failure(ex) =>
        pendingReqs.remove(reqId)
        frame.release()
        promise.tryFailure(ex)
    }
    promise.future
  }

  // 超时处理
  def push(host: String, port: Int, body: TransportCodec.PushBody, timeoutMs: Long): Future[Boolean] = {
    val f = push(host, port, body)
    scheduleTimeout(f, timeoutMs, false)
  }

  def fetch(host: String, port: Int, body: TransportCodec.FetchBody, timeoutMs: Long): Future[Array[Byte]] = {
    val f = fetch(host, port, body)
    scheduleTimeout(f, timeoutMs, Array.emptyByteArray)
  }

  def replicate(host: String, port: Int, body: TransportCodec.PushBody, timeoutMs: Long): Future[Boolean] = {
    val f = replicate(host, port, body)
    scheduleTimeout(f, timeoutMs, false)
  }

  private def scheduleTimeout[T](f: Future[T], timeoutMs: Long, default: T): Future[T] = {
    val result = Promise[T]()
    ioGroup.schedule(() => {
      if (!f.isCompleted) result.trySuccess(default)
    }, timeoutMs, TimeUnit.MILLISECONDS)
    f.foreach(v => result.trySuccess(v))
    f.failed.foreach(_ => result.trySuccess(default))
    result.future
  }

  override def close(): Unit = {
    channels.values().forEach { ch =>
      if (ch.isOpen) ch.close()
    }
    channels.clear()
    ioGroup.shutdownGracefully()
    log.info("TransportClient closed")
  }
}

/**
 * Client 响应处理器 — 解析帧并完成对应的 Promise。
 */
class ClientResponseHandler(pendingReqs: ConcurrentHashMap[Int, Promise[Any]])
  extends ChannelInboundHandlerAdapter {

  private val log = LoggerFactory.getLogger(getClass)

  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
    val buf = msg.asInstanceOf[ByteBuf]
    try {
      TransportCodec.decodeFrame(buf) match {
        case Some((msgType, reqId, body)) =>
          try {
            val promise = pendingReqs.remove(reqId)
            if (promise == null) {
              log.warn(s"No pending request for reqId=$reqId")
              return
            }
            msgType match {
              case TransportCodec.TypePushResponse =>
                val (success, err) = TransportCodec.decodePushResponse(body)
                promise.success((success, err))

              case TransportCodec.TypeFetchResponse =>
                val (success, data, err) = TransportCodec.decodeFetchResponse(body)
                promise.success((success, data, err))

              case TransportCodec.TypeReplicateRsp =>
                val (success, err) = TransportCodec.decodeReplicateResponse(body)
                promise.success((success, err))

              case _ => log.warn(s"Unexpected response type: $msgType")
            }
          } finally { body.release() }
        case None => log.warn("Incomplete response frame")
      }
    } finally { buf.release() }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    log.error("TransportClient channel exception", cause)
    ctx.close()
  }
}

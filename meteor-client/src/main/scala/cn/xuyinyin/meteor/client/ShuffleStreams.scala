package cn.xuyinyin.meteor.client

import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source, SourceQueueWithComplete}
import org.apache.pekko.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import org.apache.pekko.NotUsed

import cn.xuyinyin.meteor.common.Protocol._
import scala.concurrent.{ExecutionContext, Future, Promise}

/**
 * Pekko Streams 数据推拉管道
 *
 * 对标 Celeborn 的数据面传输：
 *   - PushData：上游 TM → Pekko Stream Source → Worker（背压保证不丢数据）
 *   - FetchData：下游 TM ← Worker ← Stream Sink（分批拉取）
 */
object ShuffleStreams {

  /**
   * 创建 Push 管道
   *
   * @param bufferSize 内部缓冲容量
   * @param onPush     推送回调
   * @param mat        流物化器
   * @param ec         执行上下文
   */
  def pushPipeline[T](
    bufferSize: Int = 512,
    onPush: (T, Promise[Boolean]) => Unit
  )(implicit mat: Materializer, ec: ExecutionContext): SourceQueueWithComplete[(T, Promise[Boolean])] = {

    Source.queue[(T, Promise[Boolean])](bufferSize, OverflowStrategy.backpressure)
      .mapAsyncUnordered(32) { case (element, promise) =>
        Future {
          onPush(element, promise)
        }.flatMap(_ => promise.future.map(_ => element))
      }
      .to(Sink.ignore)
      .run()
  }

  /**
   * Push 一个 Partition 的数据块
   */
  def pushData(
    queue: SourceQueueWithComplete[(PushData, Promise[Boolean])],
    shuffleId: ShuffleId,
    partitionIndex: Int,
    attemptNumber: Int,
    data: Array[Byte]
  )(implicit ec: ExecutionContext): Future[PushDataResponse] = {
    val promise = Promise[Boolean]()
    val req = PushData(shuffleId, partitionIndex, attemptNumber, data, checksum = 0L)
    queue.offer((req, promise)).flatMap {
      case QueueOfferResult.Enqueued =>
        promise.future.map { success =>
          PushDataResponse(shuffleId, partitionIndex, success)
        }
      case QueueOfferResult.Dropped =>
        Future.successful(PushDataResponse(shuffleId, partitionIndex, success = false))
      case QueueOfferResult.Failure(ex) =>
        Future.failed(ex)
      case QueueOfferResult.QueueClosed =>
        Future.failed(new IllegalStateException("Push queue closed"))
    }
  }

  /**
   * Fetch 管道：从 Worker 批量拉取数据
   */
  def fetchPipeline(
    fetchBatch: (FetchData, Promise[Array[Byte]]) => Unit,
    maxConcurrent: Int = 32
  )(implicit ec: ExecutionContext): Flow[FetchData, FetchDataResponse, NotUsed] = {

    Flow[FetchData].mapAsyncUnordered(maxConcurrent) { req =>
      val promise = Promise[Array[Byte]]()
      fetchBatch(req, promise)
      promise.future.map { data =>
        FetchDataResponse(req.shuffleId, req.partitionIndex, Some(data))
      }.recover {
        case _: Exception =>
          FetchDataResponse(req.shuffleId, req.partitionIndex, None)
      }
    }
  }
}

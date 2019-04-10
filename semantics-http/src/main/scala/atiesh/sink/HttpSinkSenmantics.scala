package atiesh.sink

// java
import java.net.URL
import java.util.concurrent.{ ConcurrentLinkedQueue => JCLQueue }
import java.util.concurrent.atomic.AtomicLong
// scala
import scala.util.{ Try, Failure, Success }
import scala.collection.mutable.Queue
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future, Await, Promise }
// akka
import akka.actor.{ ActorSystem, Cancellable }
import akka.stream.{ ActorMaterializer, Materializer }
import akka.stream.{ OverflowStrategy, QueueOfferResult }
import akka.stream.scaladsl.{ Sink => AkkaSink, _ }
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ HttpRequest => AkkaHttpRequest, HttpResponse => AkkaHttpResponse, _ }
import akka.util.ByteString
// internal
import atiesh.event.Event
import atiesh.statement.{ Signal, Transaction, Close, Closed }
import atiesh.utils.{ Configuration, Logging }
import atiesh.utils.http.{ HttpRequest, HttpResponse }

trait HttpSinkSemantics extends SinkSemantics with Logging { this: Sink =>
  object HttpSemanticsOpts {
    val OPT_REMOTE_URL = "remote-url"
    val OPT_REQUEUE_SIZE = "request-queue-size"
    val DEF_REQUEUE_SIZE = 128
    val OPT_REQUEST_TRANSITION_BACKPRESSURE_BOUNDARY = "request-transition-backpressure-boundary"
    val DEF_REQUEST_TRANSITION_BACKPRESSURE_BOUNDARY = 128
    val OPT_HTTPSINK_AKKA_DISPATCHER = "http-akka-dispatcher"
    val DEF_HTTPSINK_AKKA_DISPATCHER = "akka.actor.default-dispatcher"
  }
  val SIG_NEED_RETRY: Int = 1
  val SIG_REQ_COMPLETED: Int = 2

  implicit var httpExecutionContext: ExecutionContext = _
  implicit var materializer: Materializer = _

  var requestQueue: SourceQueueWithComplete[(AkkaHttpRequest, Promise[AkkaHttpResponse])] = _
  var connectionPool: Http.HostConnectionPool = _
  var processorFlow: Flow[(AkkaHttpRequest, Promise[AkkaHttpResponse]), (Try[AkkaHttpResponse], Promise[AkkaHttpResponse]), Http.HostConnectionPool] = _

  val requestRetryQueue = new JCLQueue[(HttpRequest, Promise[HttpResponse])]()
  var requestTransitionBackpressureBoundary: Long = _

  val INCREMENT_AND_GET: Long = 1L
  val DECREMENT_AND_GET: Long = -1L
  var requestInTransitionCount: Long = _
  def requestInTransition(change: Long): Long = { requestInTransitionCount += change; requestInTransitionCount }

  override def bootstrap()(implicit system: ActorSystem): Sink = {
    super.bootstrap()

    val cfg = getConfiguration
    val remoteURL   = new URL(cfg.getString(HttpSemanticsOpts.OPT_REMOTE_URL))
    val remoteProto = remoteURL.getProtocol
    val remoteHost  = remoteURL.getHost
    val remotePort  = if (remoteURL.getPort == -1) { if (remoteProto == "https") 443 else 80 } else remoteURL.getPort

    val akkaDispatcher = cfg.getString(
      HttpSemanticsOpts.OPT_HTTPSINK_AKKA_DISPATCHER,
      HttpSemanticsOpts.DEF_HTTPSINK_AKKA_DISPATCHER)
    val queueSize = cfg.getInt(HttpSemanticsOpts.OPT_REQUEUE_SIZE, HttpSemanticsOpts.DEF_REQUEUE_SIZE)

    httpExecutionContext = system.dispatchers.lookup(akkaDispatcher)
    materializer = ActorMaterializer()

    requestTransitionBackpressureBoundary = cfg.getLong(
      HttpSemanticsOpts.OPT_REQUEST_TRANSITION_BACKPRESSURE_BOUNDARY,
      HttpSemanticsOpts.DEF_REQUEST_TRANSITION_BACKPRESSURE_BOUNDARY)
    requestInTransitionCount = 0L

    processorFlow = {
      if (remoteProto == "https") {
        Http().newHostConnectionPoolHttps[Promise[AkkaHttpResponse]](remoteHost, remotePort)
        // Http().cachedHostConnectionPoolHttps[Promise[AkkaHttpResponse]](remoteHost, remotePort)
      } else {
        Http().newHostConnectionPool[Promise[AkkaHttpResponse]](remoteHost, remotePort)
        // Http().cachedHostConnectionPool[Promise[AkkaHttpResponse]](remoteHost, remotePort)
      }
    }
    val (queue, pool) =
      Source.queue[(AkkaHttpRequest, Promise[AkkaHttpResponse])](queueSize, OverflowStrategy.backpressure)
        .viaMat(processorFlow)(Keep.both)     // send request to remote host
        .to(AkkaSink.foreach({                // the result is Try[AkkaHttpResponse]
          case ((Success(response), p)) => p.success(response)
          case ((Failure(exc), p))      => p.failure(exc)
        })).run()                             // start the stream & get MaterializedValue
    requestQueue = queue
    connectionPool = pool

    this
  }

  override def process(sig: Int): Unit = {
    logger.debug("sink <{}> handle signal <{}>", getName, sig)
    sig match {
      /* handle request retry signal */
      case SIG_NEED_RETRY =>
        if (requestRetryQueue.isEmpty) {
          logger.error(
            "got illegal retry request from sink <{}>, " +
            "the request queue of retries was empty, which means you may use " +
            "a http sink with wrong implementation",
            getName)
        } else {
          try { /* just in case the queue return null */
            val (r, p) = requestRetryQueue.poll()
            requestInTransition(DECREMENT_AND_GET)
            httpRequest(r).onComplete({
              case Success(response) => p.success(response)
              case Failure(exc)      => p.failure(exc)
            })
          } catch {
            case exc: Throwable =>
              logger.error("got illegal retry request from retry request queue")
          }
        }

      /*
       * handle request completed signal
       *  things we care about in this handler:
       *    - is there any pending commit (<ack>)
       *    - is there any pending close (<stop>)
       * */
      case SIG_REQ_COMPLETED =>
        val inTrans = requestInTransition(DECREMENT_AND_GET)
        if (inTrans < requestTransitionBackpressureBoundary) {
          while (!transactions.isEmpty) {
            logger.debug(
              "sink <{}> acknowledge delayed commit transaction(s), " +
              " current <{}> request in transition", getName, inTrans)
            super.ack(transactions.dequeue())
          }
        }

        if (inTrans == 0 && !closing.isEmpty) {
          closing.map(p => stop(p))
        }

      /* handle other illegal signals */
      case _ =>
        logger.error(
          "got illegal signal num <{}> from sink <{}> " +
          "which means you may use a http sink with wrong implementation",
          sig,
          getName)
    }
  }

  /*
   * not thread safe method, don't call this inside future callback, if you wanna
   *    retry failure request, use httpRetry in future callback which is thread safe
   */
  def httpRequest(req: HttpRequest): Future[HttpResponse] = {
    val p = Promise[AkkaHttpResponse]()
    val r = AkkaHttpRequest(req.akkaHttpMethod, req.akkaUri, req.akkaHttpHeaders, req.akkaHttpEntity)

    val inTrans = requestInTransition(INCREMENT_AND_GET)
    logger.debug(
      "enqueue outgoing request <{} {}> with header(s) <{}> and queries <{}> and body <{}>, " +
      "current <{}> request in transition",
      req.method,
      req.uri,
      req.headers,
      req.queries,
      req.body,
      inTrans)

    Await.ready(requestQueue.offer((r -> p)), Duration.Inf).value.get match {
      case Success(QueueOfferResult.Enqueued)     =>
        p.future.flatMap(response => {  // the return value of this method
          val status = response.status.intValue
          val headers = response.headers.foldLeft(Map[String, String]())((hs, h) => { hs + (h.name -> h.value) })
          response.entity.dataBytes.runFold(ByteString.empty)({ case (acc, b) => { acc ++ b } }).map(content => {
            HttpResponse(status, headers, content.utf8String)
          })
        })

      case Success(QueueOfferResult.Dropped)      => Future.failed(new SinkBufferOverflowedException("request queue overflowed"))
      case Success(QueueOfferResult.Failure(exc)) => Future.failed(exc)
      case Success(QueueOfferResult.QueueClosed)  => Future.failed(new SinkClosedException("the downstream of sink was closed"))
      case Failure(exc)                           => Future.failed(exc)
    }
  }

  def httpRetry(req: HttpRequest): Future[HttpResponse] = {
    val p = Promise[HttpResponse]()
    requestRetryQueue.offer((req -> p))
    signal(SIG_NEED_RETRY)
    p.future
  }

  def httpComplete(): Long = {
    signal(SIG_REQ_COMPLETED)
    requestInTransitionCount
  }

  val transactions = new Queue[Promise[Transaction]]()
  override def ack(tran: Promise[Transaction]): Unit = {
    if (requestInTransitionCount < requestTransitionBackpressureBoundary) {
      super.ack(tran)
    } else {
      logger.info(
        "too many (total <{}>) request in transition, <{}> delay commit",
        requestInTransitionCount,
        getName)
      transactions.enqueue(tran)
      logger.debug("sink <{}> delayed transition commit <{}@{}>", getName, tran, tran.hashCode.toHexString)
    }
  }

  var closing: Option[Promise[Closed]] = None
  override def stop(closed: Promise[Closed]): Unit = {
    if (requestInTransitionCount != 0) {
      logger.info(
        "there still have <{}> request in transition of sink <{}>, delay close",
        requestInTransitionCount,
        getName)
      closing = Some(closed)
      logger.debug("sink <{}> delayed close <{}@{}>", getName, closed, closed.hashCode.toHexString)
    } else {
      logger.info("stopping http senmantic context of sink <{}>", getName)
      requestQueue.complete()

      /* best way to do that is not block for each of these futures but chain these futures,
       *  but the transform and transformWith have different api in scala 2.11 and 2.12 */
      Await.ready(requestQueue.watchCompletion(), Duration.Inf).value.get match {
        case Success(_) =>
          logger.info("request queue of HttpSinkSemantics of Sink <{}> shutdown gracefully", getName)
        case Failure(exc) =>
          logger.info(s"request queue of HttpSinkSemantics of Sink <${getName}> killed by exception", exc)
      }
      Await.ready(connectionPool.shutdown(), Duration.Inf).value.get match {
        case Success(_) =>
          logger.info("http connection pool of HttpSinkSemantics of Sink <{}> shutdown gracefully", getName)
        case Failure(exc) =>
          logger.info(s"http connection pool of HttpSinkSemantics of Sink <${getName}> killed by exception", exc)
      }
      logger.debug("sink <{}> complate close <{}@{}>", getName, closed, closed.hashCode.toHexString)
      super.stop(closed)
    }
  }
}

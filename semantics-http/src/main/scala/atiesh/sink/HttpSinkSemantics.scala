/*
 * Copyright (C) Hao Feng
 */

package atiesh.sink

// java
import java.net.URL
import java.util.concurrent.{ ConcurrentLinkedQueue => JCLQueue }
// scala
import scala.util.{ Try, Failure, Success }
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Await, Promise, Future }
// akka
import akka.actor.{ ActorSystem, Scheduler }
import akka.stream.{ ActorMaterializer, Materializer }
import akka.stream.{ OverflowStrategy, QueueOfferResult }
import akka.stream.scaladsl.{ Sink => AkkaSink, _ }
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ HttpRequest => AkkaHttpRequest,
                                  HttpResponse => AkkaHttpResponse, _ }
import akka.util.ByteString
// internal
import atiesh.event.Event
import atiesh.statement.Closed
import atiesh.utils.{ Configuration, Logging }
import atiesh.utils.http.{ HttpMessage, HttpRequest, HttpResponse }

object HttpSinkSemantics {
  object HttpSinkSemanticsOpts {
    val OPT_REMOTE_URL = "remote-url"
    val OPT_REQUEUE_SIZE = "request-queue-size"
    val DEF_REQUEUE_SIZE = 128
    val OPT_REQUEST_WITHOUT_SIZE_LIMITE = "request-without-size-limit"
    val DEF_REQUEST_WITHOUT_SIZE_LIMITE = true
    val OPT_RESPONSE_WITHOUT_SIZE_LIMITE = "response-without-size-limit"
    val DEF_RESPONSE_WITHOUT_SIZE_LIMITE = true
    val OPT_AKKA_DISPATCHER = "http-akka-dispatcher"
    val DEF_AKKA_DISPATCHER = "akka.actor.default-dispatcher"
  }
  object HttpSinkSemanticsSignals {
    val SIG_NEED_RETRY: Int = 1
    val SIG_REQ_COMPLETED: Int = 2
  }
}

trait HttpSinkSemantics
  extends SinkSemantics
  with Logging { this: Sink =>
  import Http.HostConnectionPool
  import HttpSinkSemantics.{ HttpSinkSemanticsOpts => Opts,
                             HttpSinkSemanticsSignals => Sig }

  final private[this] var httpDispatcher: String = _
  final private[this] var httpRetryScheduler: Scheduler = _
  final private[this] var httpExecutionContext: ExecutionContext = _
  final private[this] var httpMaterializer: Materializer = _

  final private[this] var httpWithoutRequestSizeLimit: Boolean = _
  final private[this] var httpWithoutResponseSizeLimit: Boolean = _

  type HttpReqQueue =
    SourceQueueWithComplete[(AkkaHttpRequest, Promise[AkkaHttpResponse])]
  final private[this] var httpRequestQueue: HttpReqQueue = _

  type HttpProcessorFlow =
    Flow[(AkkaHttpRequest, Promise[AkkaHttpResponse]),
         (Try[AkkaHttpResponse], Promise[AkkaHttpResponse]),
         Http.HostConnectionPool]
  final private[this] var httpProcessorFlow: HttpProcessorFlow = _
  final private[this] var httpConnectionPool: HostConnectionPool = _

  final def getHttpDispatcher: String = httpDispatcher
  final def getHttpExecutionContext: ExecutionContext = httpExecutionContext
  final def getHttpMaterializer: Materializer = httpMaterializer

  override def bootstrap()(implicit system: ActorSystem): Unit = {
    super.bootstrap()

    val cfg = getConfiguration

    val remoteURL   = new URL(cfg.getString(Opts.OPT_REMOTE_URL))
    val remoteProto = remoteURL.getProtocol
    val remoteHost  = remoteURL.getHost
    val remotePort  = { if (remoteURL.getPort == -1) {
                          if (remoteProto == "https") 443 else 80
                        } else remoteURL.getPort
                      }
    val queueSize = cfg.getInt(Opts.OPT_REQUEUE_SIZE, Opts.DEF_REQUEUE_SIZE)

    httpDispatcher = cfg.getString(Opts.OPT_AKKA_DISPATCHER,
                                   Opts.DEF_AKKA_DISPATCHER)
    httpExecutionContext = system.dispatchers.lookup(httpDispatcher)
    httpRetryScheduler = system.scheduler

    httpWithoutRequestSizeLimit =
      cfg.getBoolean(Opts.OPT_REQUEST_WITHOUT_SIZE_LIMITE,
                     Opts.DEF_REQUEST_WITHOUT_SIZE_LIMITE)
    httpWithoutResponseSizeLimit =
      cfg.getBoolean(Opts.OPT_RESPONSE_WITHOUT_SIZE_LIMITE,
                     Opts.DEF_RESPONSE_WITHOUT_SIZE_LIMITE)

    httpMaterializer = ActorMaterializer()
    httpProcessorFlow = {
      if (remoteProto == "https") {
        Http().newHostConnectionPoolHttps[Promise[AkkaHttpResponse]](
          remoteHost, remotePort)(httpMaterializer)
      } else {
        Http().newHostConnectionPool[Promise[AkkaHttpResponse]](
          remoteHost, remotePort)(httpMaterializer)
      }
    }
    val (queue, pool) =
      Source.queue[(AkkaHttpRequest, Promise[AkkaHttpResponse])](
        queueSize, OverflowStrategy.backpressure)
        /* send request to remote host */
        .viaMat(httpProcessorFlow)(Keep.both)
        /* the result is Try[AkkaHttpResponse] */
        .to(AkkaSink.foreach({
          case ((Success(response), p)) =>
            p.success(response)
          case ((Failure(exc), p))      =>
            p.failure(exc)
        }))
        /* start the stream & get MaterializedValue */
        .run()(httpMaterializer)
    httpRequestQueue = queue
    httpConnectionPool = pool
  }

  override def process(sig: Int): Unit = {
    logger.debug("http sink semantics of sink <{}> " +
                 "handle signal <{}>", getName, sig)

    sig match {
      /**
       * handle request retry signal.
       */
      case Sig.SIG_NEED_RETRY =>
        if (httpRetryQueue.isEmpty) {
          logger.error("got illegal retry request from sink <{}>, " +
                       "the request queue of retries was empty, " +
                       "which means you may use a http sink with " +
                       "wrong implementation", getName)
        } else {
          try { /* just in case the queue return null which should not */
            val (r, p) = httpRetryQueue.poll()
            httpRequest(r).onComplete({
              case Success(response) => p.success(response)
              case Failure(exc)      => p.failure(exc)
            })(httpExecutionContext)
          } catch {
            case exc: Throwable =>
              logger.error("got illegal retry request from " +
                           "retry request queue")
          }
        }

      /**
       * handle request completed signal.
       */
      case Sig.SIG_REQ_COMPLETED =>
        httpCompleteHandler()

      /**
       * handle other illegal signals.
       */
      case _ =>
        logger.error("http sink semantics of sink <{}> got illegal " +
                     "signal num <{}> which means you may use a " +
                     "http sink with wrong implementation", getName, sig)
    }
  }

  /**
   * Low-Level API - HttpRequest
   *
   * Not thread safe method, don't call this inside future callback,
   * if you wanna retry a request, use httpRetry in future callback
   * which is thread safe.
   */
  final def httpRequest(req: HttpRequest): Future[HttpResponse] = {
    val p = Promise[AkkaHttpResponse]()
    val r = AkkaHttpRequest(req.akkaHttpMethod,
                            req.akkaUri,
                            req.akkaHttpHeaders,
                            { if (httpWithoutRequestSizeLimit) {
                                req.akkaHttpEntity.withoutSizeLimit()
                              } else req.akkaHttpEntity
                            })

    Await.ready(
      httpRequestQueue.offer((r -> p)), Duration.Inf).value.get match {
      case Success(QueueOfferResult.Enqueued) =>
        p.future.flatMap(akkaResponse => {
          val status = akkaResponse.status.intValue
          val headers = HttpMessage.parseHeaders(akkaResponse)

          val bodyStream = {
           if (httpWithoutResponseSizeLimit) {
              akkaResponse.entity.withoutSizeLimit().dataBytes
            } else akkaResponse.entity.dataBytes
          }
          bodyStream.runFold(ByteString.empty)({ /* read response body */
              case (acc, b) => { acc ++ b }
            })(httpMaterializer)
            .map(content => {
              HttpResponse(status, headers, content)
            })(httpExecutionContext)
        })(httpExecutionContext)

      case Success(QueueOfferResult.Dropped) =>
        Future.failed(new SinkBufferOverflowedException(
                        s"sink <${getName}> request queue overflowed"))
      case Success(QueueOfferResult.QueueClosed) =>
        Future.failed(new SinkClosedException(
                        s"sink <${getName}> request queue was closed"))
      case Success(QueueOfferResult.Failure(exc)) => Future.failed(exc)
      case Failure(exc) => Future.failed(exc)
    }
  }

  /**
   * Low-Level API - HttpRetry
   *
   * Thread safe http request retry method, use it inside httpRequest method's
   *  future callback.
   */
  final private[this] val httpRetryQueue =
    new JCLQueue[(HttpRequest, Promise[HttpResponse])]()
  final def httpRetry(req: HttpRequest): Future[HttpResponse] =
    httpRetry(req, Duration(0, MILLISECONDS))
  final def httpRetry(req: HttpRequest,
                      delay: FiniteDuration): Future[HttpResponse] = {
    val p = Promise[HttpResponse]()
    httpRetryQueue.offer((req -> p))
    if (delay.toMillis <= 0) {
      signal(Sig.SIG_NEED_RETRY)
    } else {
      httpRetryScheduler.scheduleOnce(delay)({
        signal(Sig.SIG_NEED_RETRY)
      })(getHttpExecutionContext)
    }
    p.future
  }

  /**
   * Low-Level API - HttpComplete
   *
   * Thread safe http request complete signal trigger method, use it trigger
   *  httpCompleteHandler invoke in a thread safe context (inside actor).
   */
  final def httpComplete(): Unit = signal(Sig.SIG_REQ_COMPLETED)

  /**
   * Low-Level API - HttpCompleteHandler
   *
   * User defined callback hook, called after each request complete.
   */
  def httpCompleteHandler(): Unit

  override def close(closed: Promise[Closed]): Unit = {
    logger.info("sink <{}> closing http semantics context", getName)

    httpRequestQueue.complete()
    /* best way to do that is not block for each of these futures but
     *  chain these futures, but the transform and transformWith have
     *  different api in scala 2.11 and 2.12 */
    Await.ready(
      httpRequestQueue.watchCompletion(), Duration.Inf).value.get match {
      case Success(_) =>
        logger.info("request queue of HttpSinkSemantics of Sink <{}> " +
                    "shutdown gracefully", getName)
      case Failure(exc) =>
        logger.info(s"request queue of HttpSinkSemantics of Sink <" +
                    s"${getName}> killed by exception", exc)
    }
    Await.ready(httpConnectionPool.shutdown(), Duration.Inf).value.get match {
      case Success(_) =>
        logger.info("http connection pool of HttpSinkSemantics of Sink <{}> " +
                    "shutdown gracefully", getName)
      case Failure(exc) =>
        logger.info(s"http connection pool of HttpSinkSemantics of Sink <" +
                    s"${getName}> killed by exception", exc)
    }

    logger.debug("http sink semantics of sink <{}> complate " +
                 "closed promise <{}@{}>",
                 getName, closed, closed.hashCode.toHexString)
    super.close(closed)
  }
}

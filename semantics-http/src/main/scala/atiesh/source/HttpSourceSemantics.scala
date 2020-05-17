/*
 * Copyright (C) Hao Feng
 */

package atiesh.source

// java
import java.net.URL
// scala
import scala.util.{ Try, Failure, Success }
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Await, Promise, Future }
// akka
import akka.actor.ActorSystem
import akka.stream.{ ActorMaterializer, Materializer }
import akka.stream.scaladsl.{ Source => AkkaSource,
                              Sink => AkkaSink, _ }
import akka.http.scaladsl.{ Http, HttpExt }
import akka.http.scaladsl.model.{ HttpMethod => AkkaHttpMethod,
                                  HttpMethods => AkkaHttpMethods,
                                  HttpProtocol => AkkaHttpProtocol,
                                  HttpProtocols => AkkaHttpProtocols,
                                  HttpRequest => AkkaHttpRequest,
                                  HttpResponse => AkkaHttpResponse, _ }
import akka.util.ByteString
// internal
import atiesh.event.Event
import atiesh.statement.{ Ready, Closed }
import atiesh.utils.{ Configuration, Logging }
import atiesh.utils.http.{ HttpMessage, HttpRequest, HttpResponse }

object HttpSourceSemantics {
  object HttpSourceSemanticsOpts {
    val OPT_LISTEN_URL = "listen-url"
    val DEF_LISTEN_URL = "http://127.0.0.1:80"
    val OPT_SHUTDOWN_TIMEOUT = "shutdown-timeout"
    val DEF_SHUTDOWN_TIMEOUT = Duration(30, SECONDS)
    val OPT_AKKA_DISPATCHER = "http-akka-dispatcher"
    val DEF_AKKA_DISPATCHER = "akka.actor.default-dispatcher"
  }
  object HttpSourceSemanticsSignals {
    val SIG_SERVER_FATAL: Int = 1
  }
}

trait HttpSourceSemantics
  extends SourceSemantics
  with Logging { this: Source =>
  import HttpSourceSemantics.{ HttpSourceSemanticsOpts => Opts,
                               HttpSourceSemanticsSignals => Sig }
  import HttpRequest._

  final private[this] var httpDispatcher: String = _
  final private[this] var httpExecutionContext: ExecutionContext = _
  final private[this] var httpMaterializer: Materializer = _
  final private[this] var httpContext: HttpExt = _

  final private[this] var httpBindingContext: (String, String, Int) = _
  final private[this] var shutdownTimeout: FiniteDuration = _

  type HttpConnListener = AkkaSource[Http.IncomingConnection,
                                     Future[Http.ServerBinding]]
  final private[this] var httpConnListener: HttpConnListener = _
  type HttpConnProcessor = RunnableGraph[Future[Http.ServerBinding]]
  final private[this] var httpConnProcessor: HttpConnProcessor = _
  type HttpServer = Http.ServerBinding
  final private[this] var httpServer: HttpServer = _

  final def getHttpDispatcher: String = httpDispatcher
  final def getHttpExecutionContext: ExecutionContext = httpExecutionContext
  final def getHttpMaterializer: Materializer = httpMaterializer

  override def bootstrap()(implicit system: ActorSystem): Unit = {
    val cfg = getConfiguration

    val bindURL = cfg.getString(Opts.OPT_LISTEN_URL, Opts.DEF_LISTEN_URL)
    httpBindingContext = Try {
      val u =
        try { new URL(bindURL) }
        catch { case exc: Throwable =>
          throw new SourceInitializeException(
            s"source <${getName}> got error during initializing http " +
            s"semantics, can not parse invalid <listen-url>: <${bindURL}>",
            exc)
        }
      val host = {
        if (u.getHost == "") {
          throw new SourceInitializeException(
            s"source <${getName} got error during initializing http " +
            s"semantics, bad <listen-url> <${bindURL}> with empry host part")
          } else u.getHost
      }
      val proto = u.getProtocol
      val port  = {   /* TODO: at this point, https may not work properly */
        if (u.getPort == -1) if (proto == "https") 443 else 80
        else u.getPort
      }
      (proto, host, port)
    } match {
      case Success(parts) => parts
      case Failure(exc) => throw exc
    }
    shutdownTimeout = cfg.getDuration(Opts.OPT_SHUTDOWN_TIMEOUT,
                                      Opts.DEF_SHUTDOWN_TIMEOUT)

    httpDispatcher = cfg.getString(Opts.OPT_AKKA_DISPATCHER,
                                   Opts.DEF_AKKA_DISPATCHER)
    httpExecutionContext = system.dispatchers.lookup(httpDispatcher)
    httpMaterializer = ActorMaterializer()
    httpContext = Http()

    val (bindProto, bindHost, bindPort) = httpBindingContext
    httpConnProcessor =
      httpConstructServerProcessor(
        bindProto, bindHost, bindPort)(httpMaterializer)

    super.bootstrap()
  }

  override def open(ready: Promise[Ready]): Unit = {
    httpServer =
      Await.ready(httpConnProcessor.run()(httpMaterializer),
                  Duration.Inf).value.get match {
        case Success(binding) =>
          logger.debug(
            "source <{}> http server stream binding successful", getName)
          binding
        case Failure(exc) =>
          throw new SourceInitializeException(
            s"source <${getName}> got fatal error during http " +
            s"server binding, abort initialize", exc)
      }
    super.open(ready)
  }

  override def process(sig: Int): Unit = {
    logger.debug("source <{}> handle http signal <{}>", getName, sig)

    sig match {
      /**
       * handle server fatal signal.
       */
      case Sig.SIG_SERVER_FATAL =>
        logger.warn("source <{}> got http server fatal signal, " +
                    "restarting http server stream", getName)
        val (bindProto, bindHost, bindPort) = httpBindingContext
        httpConnProcessor =
          httpConstructServerProcessor(
            bindProto, bindHost, bindPort)(httpMaterializer)

        Await.ready(httpConnProcessor.run()(httpMaterializer),
                    Duration.Inf).value.get match {
          case Success(binding) =>
            logger.debug(
              "source <{}> http server stream binding successful", getName)
            httpServer = binding
          case Failure(exc) =>
            logger.error(s"source <${getName}> got fatal error during " +
                         s"binding while restart http server stream, " +
                         s"retry again", exc)
            signal(Sig.SIG_SERVER_FATAL)
        }

      /**
       * handle other illegal signals.
       */
      case _ =>
        logger.error("source <{}> got illegal signal num <{}> which means " +
                     "you may use a http source with wrong implementation",
                     getName, sig)
    }
  }

  final private[this] def httpConstructServerProcessor(
    bindProto: String, bindHost: String, bindPort: Int)(
    implicit fm: Materializer): HttpConnProcessor = {
    httpConnListener = {
      if (bindProto == "http") {
        httpContext.bind(bindHost, bindPort)
      } else {
        throw new SourceInitializeException(
          s"source <${getName}> got error during initializing http " +
          s"semantics, non http protocols current not supported, these " +
          s"protocols (e.g.: https, http2) should handled by layer 7 " +
          s"proxy & balancer servers")
      }
    }
    httpConnListener
      .via(Flow[Http.IncomingConnection].watchTermination()((_, term) => {
        term.failed.map(exc => {       /* handle server stream fatal */
          logger.error(s"source <${getName}> got http fatal error, the " +
                       s"http server stream was stopped unexpectedly", exc)
          signal(Sig.SIG_SERVER_FATAL)
        })(httpExecutionContext)
      }))
      .to(AkkaSink.foreach(conn => {   /* handle incoming connections */
        logger.debug("source <{}> accepted new connection " +
                     "from <{}>", getName, conn.remoteAddress)
        conn.handleWith(
          Flow[AkkaHttpRequest]
            .via(Flow[AkkaHttpRequest] /* handle connection fatal */
              .recover[AkkaHttpRequest]({
                case exc =>
                  httpConnErrorHandler(exc).akkaHttpRequest
              }))
            .mapAsync(1)(httpRequestAkkaHandler(_))
        )
      }))
  }

  final private def httpRequestAkkaHandler(
    akkaRequest: AkkaHttpRequest): Future[AkkaHttpResponse] = {
    val p = Promise[AkkaHttpResponse]()
    akkaRequest match {
      case AkkaHttpRequest(method, akkaUri, akkaHeaders, akkaEntity, _) =>
        val (queries, uri) = HttpMessage.parseUri(akkaUri)
        val headers = HttpMessage.parseHeaders(akkaHeaders)
        akkaEntity.dataBytes
          .runFold(ByteString.empty)(_ ++ _)(httpMaterializer)
          .map(bs => {
            new HttpRequest(uri, queries, method, headers, bs)
          })(httpExecutionContext)
          .map(req => httpRequestFilter(req))(httpExecutionContext)
          .flatMap(req => httpRequestHandler(req))(httpExecutionContext)
          .recover(httpRequestErrorHandler)(httpExecutionContext)
          .onComplete({
            case Success(response) =>
              p.success(response.akkaHttpResponse)
            case Failure(exc) =>
              p.failure(exc)
          })(httpExecutionContext)
    }
    p.future
  }

  def httpRequestHandler(req: HttpRequest): Future[HttpResponse] =
    Try { httpRequestExtractEvents(req) } match {
      case Success(events) =>
        scheduleNextCycle(events)
          .map(_ => {
            httpRequestRespond(req, events)
          })(httpExecutionContext)
      case Failure(exc) =>
        Future.failed(exc)
    }

  /**
   * Not thread safe method, user defined filter method for filter each
   * incoming request, discard request when throw exception.
   *
   * Any exception thrown here, will be caught by httpRequestErrorHandler
   * later for decide the proper response to send to the client.
   */
  def httpRequestFilter(req: HttpRequest): HttpRequest

  /**
   * Not thread safe method, user defined event(s) extract method for
   * extract event(s) from each incoming request to the atiesh event engine.
   *
   * Any exception thrown here, will be caught by httpRequestErrorHandler
   * later for decide the proper response to send to the client.
   */
  def httpRequestExtractEvents(req: HttpRequest): List[Event]

  /**
   * Not thread safe method, user defined respond method for send response
   * to the client for each incoming request.
   *
   * Any exception thrown here, will be caught by httpRequestErrorHandler
   * later for decide the proper response to send to the client.
   */
  def httpRequestRespond(req: HttpRequest,
                         events: List[Event]): HttpResponse

  /**
   * Not thread safe method, user defined request level error handler, no
   * matter what happened, if there is an exception during request => response
   * process, this handler called.
   *
   * see also:
   *  - httpRequestFilter
   *  - httpRequestExtractEvents
   *  - httpRequestRespond
   */
  def httpRequestErrorHandler: PartialFunction[Throwable, HttpResponse]

  /**
   * Not thread safe method, user defined connection level error handler, no
   * matter what happened, if there is an exception during reading request
   * from an incomming connection, this handler called.
   */
  def httpConnErrorHandler: PartialFunction[Throwable, HttpRequest]

  override def close(closed: Promise[Closed]): Unit = {
    logger.info("source <{}> closing http semantics context", getName)

    /* best way to do that is not block for each of these futures but
     *  chain these futures, but the transform and transformWith have
     *  different api in scala 2.11 and 2.12 */
    Await.ready(
      httpServer.unbind(), Duration.Inf).value.get match {
      case Success(_) =>
        logger.info(
          "http server of source <{}> unbind socket", getName)
      case Failure(exc) =>
        logger.info(
          s"http server of source <${getName}> unbind failed", exc)
    }
    Await.ready(
      httpServer.terminate(shutdownTimeout), Duration.Inf).value.get match {
      case Success(_) =>
        logger.info(
          "http server of source <{}> shutdown gracefully", getName)
      case Failure(exc) =>
        logger.info(
          s"http server of source <${getName}> killed by exception", exc)
    }

    logger.debug("http source semantics of source <{}> complate " +
                 "closed promise <{}@{}>",
                 getName, closed, closed.hashCode.toHexString)
    super.close(closed)
  }
}

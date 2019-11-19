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
import akka.http.scaladsl.settings.ServerSettings
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
    val OPT_MAX_CONNECTIONS = "max-connections"
    val DEF_MAX_CONNECTIONS = 8
    val OPT_REQUEST_WITHOUT_SIZE_LIMITE = "request-without-size-limit"
    val DEF_REQUEST_WITHOUT_SIZE_LIMITE = true
    val OPT_RESPONSE_WITHOUT_SIZE_LIMITE = "response-without-size-limit"
    val DEF_RESPONSE_WITHOUT_SIZE_LIMITE = true

    /**
     * the mystery 503:
     *    https://github.com/akka/akka-http/issues/2184
     */
    val OPT_WITHOUT_REQUEST_TIMEOUT = "without-request-timeout"
    val DEF_WITHOUT_REQUEST_TIMEOUT = true

    val OPT_AKKA_DISPATCHER = "http-akka-dispatcher"
    val DEF_AKKA_DISPATCHER = "akka.actor.default-dispatcher"
  }
  type HttpConnProcessor = RunnableGraph[Future[Http.ServerBinding]]

  val HttpInternalServerErrorResponse =
    HttpResponse(500, Map[String, String](), "Internal Server Error")
      .akkaHttpResponse
}

trait HttpSourceSemantics
  extends SourceSemantics
  with Logging { this: Source =>
  import HttpSourceSemantics.{ HttpSourceSemanticsOpts => Opts, _ }
  import HttpRequest._

  final private[this] var httpDispatcher: String = _
  final private[this] var httpExecutionContext: ExecutionContext = _
  final private[this] var httpMaterializer: Materializer = _

  final private[this] var httpConnProcessor: HttpConnProcessor = _
  final private[this] var httpServer: Http.ServerBinding = _

  final private[this] var httpShutdownTimeout: FiniteDuration = _
  final private[this] var httpWithoutRequestSizeLimit: Boolean = _
  final private[this] var httpWithoutResponseSizeLimit: Boolean = _

  final def getHttpDispatcher: String = httpDispatcher
  final def getHttpExecutionContext: ExecutionContext = httpExecutionContext
  final def getHttpMaterializer: Materializer = httpMaterializer

  override def bootstrap()(implicit system: ActorSystem): Unit = {
    super.bootstrap()

    val cfg = getConfiguration

    val bindURL = cfg.getString(Opts.OPT_LISTEN_URL, Opts.DEF_LISTEN_URL)
    val (bindProto, bindHost, bindPort) = Try {
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
      if (proto != "http") {
        throw new SourceInitializeException(
          s"source <${getName}> got error during initializing http " +
          s"semantics, non http protocols current not supported, these " +
          s"protocols (e.g.: https, http2) should handled by layer 7 " +
          s"proxy & balancer servers")
      }
      val port  = {
        if (u.getPort == -1) if (proto == "https") 443 else 80
        else u.getPort
      }
      (proto, host, port)
    } match {
      case Success(parts) => parts
      case Failure(exc) => throw exc
    }
    val bindSettings = {
      if (cfg.getBoolean(Opts.OPT_WITHOUT_REQUEST_TIMEOUT,
                         Opts.DEF_WITHOUT_REQUEST_TIMEOUT)) {
        ServerSettings(system).mapTimeouts(_.withRequestTimeout(Duration.Inf))
      } else {
        ServerSettings(system)
      }
    }
    val maxConnections = cfg.getInt(Opts.OPT_MAX_CONNECTIONS,
                                    Opts.DEF_MAX_CONNECTIONS)

    httpShutdownTimeout =
      cfg.getDuration(Opts.OPT_SHUTDOWN_TIMEOUT,
                      Opts.DEF_SHUTDOWN_TIMEOUT)
    httpWithoutRequestSizeLimit =
      cfg.getBoolean(Opts.OPT_REQUEST_WITHOUT_SIZE_LIMITE,
                     Opts.DEF_REQUEST_WITHOUT_SIZE_LIMITE)
    httpWithoutResponseSizeLimit =
      cfg.getBoolean(Opts.OPT_RESPONSE_WITHOUT_SIZE_LIMITE,
                     Opts.DEF_RESPONSE_WITHOUT_SIZE_LIMITE)

    httpDispatcher = cfg.getString(Opts.OPT_AKKA_DISPATCHER,
                                   Opts.DEF_AKKA_DISPATCHER)
    httpExecutionContext = system.dispatchers.lookup(httpDispatcher)
    httpMaterializer = ActorMaterializer()

    httpConnProcessor =
      /*
       * construct the server stream (a source of incoming http connections):
       *   Source[Http.IncomingConnection,
       *          Future[akka.http.scaladsl.Http.ServerBinding]]
       *          || (react to the top level server fatal)
       *   Flow[Http.IncomingConnection]
       *          || (control server parallelism, applying stream backpressure)
       *   mapAsync(maxConnections)(conn => {...})
       *          || (make it runnable)
       *   to(AkkaSink.ignore)
       *          || (stream is runnable, matValue: Future[Http.ServerBinding])
       *   RunnableGraph[Future[Http.ServerBinding]])
       */
      Http().bind(bindHost, bindPort, settings = bindSettings)
        .via(Flow[Http.IncomingConnection].watchTermination()((_, term) => {
          term.failed.map(exc => {        /* the server stream fatal */
            logger.error(s"source <${getName}> got http fatal error, the " +
                         s"http server stream was stopped unexpectedly", exc)
          })(httpExecutionContext)
        }))
        .mapAsync(maxConnections)(conn => {
          logger.debug("source <{}> accepted new connection " +
                       "from <{}>", getName, conn.remoteAddress)
          /*
           * the return value of handleWith method of IncomingConnection is the
           * matValue of connection handle Flow which is represented as:
           *
           *   Flow[AkkaHttpRequest, AkkaHttpResponse, Future[akka.Done]]
           *
           * by using the matValue of the connection stream to indicate the
           * connection handle state (which should be a scala future instance),
           * we can fulfill the asynchronous stream semantics of the mayAsync
           * operator.
           *
           * and thanks to the mapAsync, now we have the ability to control the
           * server parallelism and applying stream backpressure to the server
           * source.
           */
          conn.handleWith(
            /*
             * construct the connection stream to handle each incoming http
             * request, materializes the stream to Future[akka.Done] that
             * completes on stream termination to indicates the connection
             * was closed (handled).
             */
            Flow[AkkaHttpRequest]
              .via(Flow[AkkaHttpRequest] /* react to connection stream fatal */
                .recover[AkkaHttpRequest]({
                  case exc: Throwable =>
                    httpConnErrorHandler(exc).akkaHttpRequest
                }))
              .mapAsync(1)(req => httpRequestAkkaHandler(req))
              /* react to connection stream terminate (connection closed) */
              .watchTermination()((_, done) => done))(httpMaterializer)
        })
        .to(AkkaSink.ignore)
  }

  /**
   * TODO: Invoke <httpRequestFilter> before read the request body.
   */
  final private def httpRequestAkkaHandler(
    akkaRequest: AkkaHttpRequest): Future[AkkaHttpResponse] = {
    val p = Promise[AkkaHttpResponse]()
    akkaRequest match {
      case AkkaHttpRequest(method, akkaUri, akkaHeaders, akkaEntity, _) =>
        val (queries, uri) = HttpMessage.parseUri(akkaUri)
        val headers = HttpMessage.parseHeaders(akkaHeaders)

        val bodyStream = {
          if (httpWithoutRequestSizeLimit) {
            akkaEntity.withoutSizeLimit().dataBytes
          } else akkaEntity.dataBytes
        }
        bodyStream.runFold(ByteString.empty)(_ ++ _)(httpMaterializer)
          .map(bs => {
            new HttpRequest(uri, queries, method, headers, bs)
          })(httpExecutionContext)
          .map(req => httpRequestFilter(req))(httpExecutionContext)
          .flatMap(req => httpRequestHandler(req))(httpExecutionContext)
          .recover(httpRequestErrorHandler)(httpExecutionContext)
          .onComplete({
            case Success(response) =>
              p.success(
                response.akkaHttpResponse.mapEntity(entity => {
                  if (httpWithoutResponseSizeLimit) {
                    entity.withoutSizeLimit()
                  } else entity
                }))
            case Failure(exc) =>
              logger.error(s"source <${getName}> got unexpected exception " +
                           s"during request process cycle and not recover " +
                           s"with httpRequestErrorHandler, response with " +
                           s"default InternalServerError response", exc)
              p.success(HttpInternalServerErrorResponse)
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

  override def close(closed: Promise[Closed]): Unit = {
    logger.info("source <{}> closing http semantics context", getName)

    /* best way to do that is not block for each of these futures but
     *  chain these futures, but the transform and transformWith have
     *  different api in scala 2.11 and 2.12 */
    Await.ready(
      httpServer.unbind(), Duration.Inf).value.get match {
      case Success(_) =>
        logger.info(
          "source <{}> http server unbind socket", getName)
      case Failure(exc) =>
        logger.info(
          s"source <${getName}> http server unbind failed", exc)
    }
    Await.ready(
      httpServer.terminate(httpShutdownTimeout), Duration.Inf).value.get match {
      case Success(_) =>
        logger.info(
          "source <{}> http server shutdown gracefully", getName)
      case Failure(exc) =>
        logger.info(
          s"source <${getName}> http server killed by exception", exc)
    }

    logger.debug("http source semantics of source <{}> complate " +
                 "closed promise <{}@{}>",
                 getName, closed, closed.hashCode.toHexString)
    super.close(closed)
  }
}

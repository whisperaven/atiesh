/*
 * Copyright (C) Hao Feng
 */

package atiesh.source

// java
import java.nio.charset.Charset
import java.net.InetSocketAddress
// scala
import scala.concurrent.Future
import scala.util.{ Try, Success, Failure }
// internal 
import atiesh.interceptor.Interceptor
import atiesh.sink.Sink
import atiesh.statement.Confirmation
import atiesh.event.{ Event, SimpleEvent }
import atiesh.utils.{ Configuration, Logging, Compressor }
import atiesh.utils.http.{ HttpMessage, HttpRequest, HttpResponse,
                           HttpBadRequestException }

object HttpSource {
  /* predefine successful response codes */
  val HTTP_SUCCESS_RESPONSE_CODE = 201
  /* predefine failed response codes */
  val HTTP_BAD_REQUEST_RESPONSE_CODE         = 400
  val HTTP_INTERNAL_ERROR_RESPONSE_CODE      = 500
  val HTTP_SERVICE_UNAVAILABLE_RESPONSE_CODE = 503

  object HttpSourceOpts {
    val OPT_HTTP_REQ_CODEC = "request-codec"
    val DEF_HTTP_REQ_CODEC = HttpMessage.charsetName

    val OPT_HTTP_REQ_BODY_EVENT_DELIMITER = "request-body-event-delimiter"
    val DEF_HTTP_REQ_BODY_EVENT_DELIMITER = "\n"

    /**
     * scan and capture http headers of incoming request which starts
     * with this prefix and put them into the atiesh event header
     * before submit to sink component.
     */
    val OPT_HTTP_REQ_HEADER_CAPTURE_PREFIX = "request-header-capture-prefix"
  }
}

class HttpSource(name: String,
                 dispatcher: String,
                 cfg: Configuration,
                 interceptors: List[Interceptor],
                 sinks: List[Sink],
                 strategy: String)
  extends AtieshSource(name, dispatcher, cfg, interceptors, sinks, strategy)
  with PassiveSourceSemantics
  with HttpSourceSemantics
  with Logging {
  import HttpSource.{ HttpSourceOpts => Opts, _ }

  val cfgReqHeaderCapturePrefix =
    cfg.getStringOption(Opts.OPT_HTTP_REQ_HEADER_CAPTURE_PREFIX)
  val cfgReqBodyEventDelimiter = {
    val delimiter = cfg.getString(Opts.OPT_HTTP_REQ_BODY_EVENT_DELIMITER,
                                  Opts.DEF_HTTP_REQ_BODY_EVENT_DELIMITER)
    if (delimiter.length == 1) delimiter.head
    else {
      throw new SourceInitializeException(
        s"cannot initialize http source component with given " +
        s"delimiter <${delimiter}>, which should be single char " +
        s"not a string with length <${delimiter.length}>, you may want " +
        s"check the setting of " +
        s" <${Opts.OPT_HTTP_REQ_BODY_EVENT_DELIMITER}> to a valid one")
    }
  }

  val cfgReqCodec = {
    val charsetName = cfg.getString(Opts.OPT_HTTP_REQ_CODEC,
                                    Opts.DEF_HTTP_REQ_CODEC)
    try {
      Charset.forName(charsetName)
    } catch {
      case exc: Throwable =>
        throw new SourceInitializeException(
          s"cannot initialize http source component with given " +
          s"charset <${charsetName}>, which is illegal or unsupported " +
          s"name, you may want change the setting of " +
          s"<${Opts.OPT_HTTP_REQ_CODEC}> to a valid one", exc)
    }
  }

  /**
   * Request Assumption:
   *  RequestLine => POST / Http/1.1
   */
  def httpRequestFilter(req: HttpRequest): HttpRequest = req

  /**
   * Request Assumption:
   *  RequestBody => Record\nRecord\n...
   */
  def httpRequestExtractEvents(req: HttpRequest): List[Event] =
    req.headers.get("content-encoding")
      .flatMap(encoding => {
        logger.debug("source <{}> handle header <Content-Encoding>, got " +
                     "<{}> encoded request", getName, encoding)
        if (encoding.toLowerCase == "gzip") {
          logger.debug("source <{}> handle gzip decompress", getName)
          Compressor.gzipDecompress(req.body.toArray) match {
            case Success(bs) => Some(new String(bs, cfgReqCodec))
            case Failure(exc) =>
              throw new HttpBadRequestException(exc.getMessage, exc)
          }
        } else {
          throw new HttpBadRequestException(
            s"got unexcepted compress format <${encoding}> in request")
        }
      })
      .getOrElse(req.stringBody)
      .split(cfgReqBodyEventDelimiter)
      .foldLeft(List[Event]())((es, payload) => {
        val headers = cfgReqHeaderCapturePrefix.map(p => {
          req.headers.foldLeft(Map[String, String]())({
            case (h, (k, v)) =>
              if (k.toLowerCase.startsWith(p.toLowerCase)) {
                h + (k.toLowerCase -> v)
              } else h
          })
        }).getOrElse(Map[String, String]())
        SimpleEvent(payload, headers) :: es
      })

  /**
   * The normal http request responder, error response not initialize here.
   */
  def httpRequestRespond(req: HttpRequest, events: List[Event],
                         confirm: Future[Confirmation]): Future[HttpResponse] =
    Future.successful(
      HttpResponse(HTTP_SUCCESS_RESPONSE_CODE,
                   Map[String, String](), HttpMessage.emptyBody))

  /**
   * The http error responder, all error handled here.
   */
  def httpRequestErrorHandler: PartialFunction[Throwable, HttpResponse] = {
    case exc: HttpBadRequestException =>
      logger.error(s"source <${getName}> unable to parse incoming request, " +
                   s"response with status code 400", exc)
      HttpResponse(HTTP_BAD_REQUEST_RESPONSE_CODE, Map[String, String](),
                        s"Bad Request, ${exc.getMessage}")
    case exc: HttpTooManyRequestException =>
      logger.error(s"source <${getName}> unable to process incoming request, " +
                   s"too many requests", exc)
      HttpResponse(HTTP_SERVICE_UNAVAILABLE_RESPONSE_CODE,
                   Map[String, String](),
                   s"Too Many Requests, ${exc.getMessage}")
    case _ => HttpResponse(HTTP_INTERNAL_ERROR_RESPONSE_CODE,
                           Map[String, String](), "Internal Server Error")
  }

  /**
   * The http connection errors, connection level means no request read
   * or no need for response write, after some necessary things (e.g.:
   * log, trace), just throw the exception to the caller.
   */
  def httpConnErrorHandler: PartialFunction[Throwable, HttpRequest] = {
    case exc: Throwable =>
      logger.error(s"http source <${getName}> got unexcepted connection " +
                   s"error, cannot process current request, exception:", exc)
      throw exc
  }

  /**
   * The http connection accept hook, whatever a connection was accepted, this
   * method was called.
   */
  def httpConnOnAccept(remoteAddress: InetSocketAddress,
                       localAddress: InetSocketAddress): Unit =
    logger.debug("source <{}> accepted new connection " +
                 "from <{} (remote)> - <{} (local)>",
                 getName, remoteAddress, localAddress)

  /**
   * The http connection terminate hook, whatever a connection was closed,
   * the partial function returned by this method was provided as a scala
   * future callback
   * (e.g.: conn.doneFuture.onComplete(httpConnOnTerminate(remote, local)).
   */
  def httpConnOnTerminate(
    remoteAddress: InetSocketAddress,
    localAddress: InetSocketAddress): PartialFunction[Try[_], Unit] = {
    case Success(_) =>
      logger.debug("source <{}> closeing connection of <{} (remote)> - <{}" +
                   " (local)>", getName, remoteAddress, localAddress)
    case Failure(exc) =>
      logger.error(s"source <${getName}> got unexcepted exception while " +
                   s"handle connection of <${remoteAddress} (remote)>" +
                   s" - <${localAddress} (local)>", exc)
  }

  def shutdown(): Unit = logger.info("shutting down http source <{}>", getName)

  def startup(): Unit = logger.info("starting http source <{}>", getName)
}

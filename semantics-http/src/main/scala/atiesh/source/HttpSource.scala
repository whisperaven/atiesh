/*
 * Copyright (C) Hao Feng
 */

package atiesh.source

// java
import java.nio.charset.Charset
// scala
import scala.concurrent.Future
import scala.util.{ Success, Failure }
// internal 
import atiesh.interceptor.Interceptor
import atiesh.sink.Sink
import atiesh.event.{ Event, SimpleEvent }
import atiesh.utils.{ Configuration, Logging, Compressor }
import atiesh.utils.http.{ HttpMessage, HttpRequest, HttpResponse,
                           HttpBadRequestException }

object HttpSource {
  val HTTP_SUCCESS_RESPONSE_CODE        = 201
  val HTTP_BAD_REQUEST_RESPONSE_CODE    = 400
  val HTTP_INTERNAL_ERROR_RESPONSE_CODE = 500

  object HttpSourceOpts {
    val OPT_HTTP_REQ_CODEC = "request-codec"
    val DEF_HTTP_REQ_CODEC = HttpMessage.charsetName

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
      .split('\n')
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
  def httpRequestRespond(req: HttpRequest,
                         events: List[Event]): HttpResponse =
    HttpResponse(HTTP_SUCCESS_RESPONSE_CODE,
                 Map[String, String](),
                 HttpMessage.emptyBody)

  /**
   * The http error responder, all error handled here.
   */
  def httpRequestErrorHandler: PartialFunction[Throwable, HttpResponse] = {
    case exc: HttpBadRequestException =>
      logger.error(s"source <${getName}> unable to parse incoming request, " +
                   s"response with status code 400", exc)
      HttpResponse(HTTP_BAD_REQUEST_RESPONSE_CODE, Map[String, String](),
                        s"Bad Request, ${exc.getMessage}")
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

  def shutdown(): Unit = logger.info("shutting down http source <{}>", getName)

  def startup(): Unit = logger.info("starting http source <{}>", getName)
}

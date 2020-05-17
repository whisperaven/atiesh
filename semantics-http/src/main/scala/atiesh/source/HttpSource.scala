/*
 * Copyright (C) Hao Feng
 */

package atiesh.source

// scala
import scala.concurrent.Future
// internal 
import atiesh.interceptor.Interceptor
import atiesh.sink.Sink
import atiesh.event.{ Event, SimpleEvent }
import atiesh.utils.{ Configuration, Logging }
import atiesh.utils.http.{ HttpMessage, HttpRequest, HttpResponse }

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

  /**
   * Request Assumption:
   *  RequestLine => POST / Http/1.1
   */
  def httpRequestFilter(req: HttpRequest): HttpRequest = req

  /**
   * Request Assumption:
   *  RequestBody => ${record}\n${record}\n...
   */
  def httpRequestExtractEvents(req: HttpRequest): List[Event] =
    req.stringBody.split('\n').foldLeft(List[Event]())((es, payload) => {
      SimpleEvent(payload, Map[String, String]()) :: es
    })

  /**
   * The normal http request responder, error response not initialize here.
   */
  def httpRequestRespond(req: HttpRequest,
                         events: List[Event]): HttpResponse = {
    HttpResponse(201, Map[String, String](), HttpMessage.emptyBody)
  }

  /**
   * The http error responder, all error handled here.
   */
  def httpRequestErrorHandler: PartialFunction[Throwable, HttpResponse] = {
    case _ => HttpResponse(500, Map[String, String](), "Internal Server Error")
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

  def doneCycle(): Unit = logger.debug("http source <{}> request cycle " +
                                       "done, prepare for handle next " +
                                       "request", getName)

  def shutdown(): Unit = logger.info("shutting down http source <{}>", getName)

  def startup(): Unit = logger.info("starting http source <{}>", getName)
}

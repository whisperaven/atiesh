/*
 * Copyright (C) Hao Feng
 */

package atiesh.sink

// java
import java.net.URL
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ ConcurrentHashMap => JCHashMap }
// scala
import scala.util.{ Try, Failure, Success }
import scala.concurrent.duration._
import scala.concurrent.Promise
import scala.collection.JavaConverters._
// internal
import atiesh.event.Event
import atiesh.statement.{ Ready, Transaction, Closed }
import atiesh.utils.{ Configuration, Logging }
import atiesh.utils.http.{ HttpRequest, HttpResponse }

object HttpLimitRequestSinkSemantics {
  object HttpLimitRequestSinkSemanticsOpts {
    val OPT_REQUEST_LIMITS = "request-limits"
    val DEF_REQUEST_LIMITS = 512
  }
  object HttpOperationStates extends Enumeration {
    type HttpOperationState = Value
    val HTTP_RETRY, HTTP_DONE = Value
  }
}

trait HttpLimitRequestSinkSemantics
  extends HttpSinkSemantics
  with Logging { this: Sink =>
  import HttpLimitRequestSinkSemantics.{
    HttpLimitRequestSinkSemanticsOpts => Opts, _ }
  import HttpOperationStates._

  final private[this] val httpRequests: AtomicLong = new AtomicLong(0)
  @volatile final private[this] var httpRequestLimits: Long = 0

  /**
   * High-Level API - HttpEnqueueRequest
   *
   * Not thread safe method, enqueue http request to http processor queue.
   */
  final def httpEnqueueRequest(req: HttpRequest,
                               events: List[Event],
                               retries: Int): Unit = {
    httpRequests.incrementAndGet()
    httpRequest(req).onComplete(response => {
      httpResponseFutureHandler(req, response, events, retries)
    })(getHttpExecutionContext)
  }

  final def httpEnqueueRequest(req: HttpRequest, event: Event): Unit =
    httpEnqueueRequest(req, List(event), 1)
  final def httpEnqueueRequest(req: HttpRequest, events: List[Event]): Unit =
    httpEnqueueRequest(req, events, 1)
  final def httpEnqueueRequest(req: HttpRequest,
                               event: Event,
                               retries: Int): Unit =
    httpEnqueueRequest(req, List(event), retries)

  final private def httpEnqueueRetryRequest(req: HttpRequest,
                                            response: Try[HttpResponse],
                                            events: List[Event],
                                            retries: Int): Unit = {
    if (retries == 0) {
      httpResponseHandler(events)(Try({
        response match {
          case Success(r) => /* protocol success but application error */
            throw new HttpMaxRetryException("request max retry reached", r)
          case Failure(exc) =>
            throw new HttpMaxRetryException("request max retry reached", exc)
        }
      })) match {
        case HTTP_RETRY =>
          logger.warn("httpResponseHandler return HTTP_RETRY but " +
                      "max retry reached, ignore and discard request")
        case _ =>
          logger.debug("httpResponseHandler return HTTP_DONE buy " +
                       "max retry reached, discard request quietly")
      }
      httpRequests.decrementAndGet()
      httpComplete()
    } else {
      httpRetry(req).onComplete(response => {
        httpResponseFutureHandler(req, response, events, retries)
      })(getHttpExecutionContext)
    }
  }

  final private def httpResponseFutureHandler(request: HttpRequest,
                                              response: Try[HttpResponse],
                                              events: List[Event],
                                              retries: Int): Unit =
    httpResponseHandler(events)(response) match {
      case HTTP_DONE =>
        httpRequests.decrementAndGet()
        httpComplete()
      case HTTP_RETRY =>
        httpEnqueueRetryRequest(request, response, events, retries - 1)
    }

  /**
   * High-Level API - HttpHandleResponse
   *
   * Not thread safe method, user defined callback for handle remote response.
   */
  def httpResponseHandler(
    events: List[Event]): PartialFunction[Try[HttpResponse],
                                          HttpOperationState]

  /**
   * Low-Level API - HttpCompleteHandler.
   *
   * Handle request completed signal, things we care about in this handler:
   *    - is there any pending commit (<ack>)
   *    - is there any pending close (<close>)
   */
  def httpCompleteHandler(): Unit = {
    val requests = httpRequests.decrementAndGet()
    if (requests < httpRequestLimits && !transactions.isEmpty) {
      transactions.iterator().asScala
        .foreach(tran => {
          logger.debug("sink <{}> acknowledge delayed commit " +
                       "transaction(s), current <{}> open requests",
                       getName, requests)
          super.ack(tran)
        })
      transactions.clear()
    }

    if (requests == 0) closing.map(closed => close(closed))
  }

  override def open(ready: Promise[Ready]): Unit = {
    httpRequestLimits = getConfiguration.getLong(Opts.OPT_REQUEST_LIMITS,
                                                 Opts.DEF_REQUEST_LIMITS)
    super.open(ready)
  }

  final private[this] val transactions =
    JCHashMap.newKeySet[Promise[Transaction]]
  override def ack(tran: Promise[Transaction]): Unit = {
    if (httpRequests.get() < httpRequestLimits) {
      super.ack(tran)
    } else {
      logger.debug("sink <{}> got too many (total <{}>) open request " +
                   "exceed request-limits, going to delay transition " +
                   "commit <{}@{}>", getName, httpRequests.get, tran,
                                     tran.hashCode.toHexString)
      transactions.add(tran)
    }
  }

  @volatile final private[this] var closing: Option[Promise[Closed]] = None
  override def close(closed: Promise[Closed]): Unit =
    if (httpRequests.get() != 0) {
      logger.info("sink <{}> still have <{}> open requests, " +
                  "delay close <{}@{}>", getName, httpRequests.get(), closed,
                                         closed.hashCode.toHexString)
      closing = Some(closed)
    } else {
      super.close(closed)
    }
}

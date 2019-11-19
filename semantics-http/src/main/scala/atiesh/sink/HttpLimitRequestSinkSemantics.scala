/*
 * Copyright (C) Hao Feng
 */

package atiesh.sink

// java
import java.net.URL
import java.io.{ BufferedWriter, OutputStreamWriter, FileOutputStream }
import java.nio.file.{ Path, Paths }
import java.util.Base64
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ ConcurrentHashMap => JCHashMap,
                              ConcurrentLinkedQueue => JCLQueue }
// scala
import scala.math
import scala.util.{ Try, Failure, Success, Random }
import scala.concurrent.duration._
import scala.concurrent.Promise
import scala.collection.JavaConverters._
// internal
import atiesh.event.{ Event, SimpleEvent }
import atiesh.statement.{ Ready, Transaction, Closed }
import atiesh.utils.{ Configuration, Logging }
import atiesh.utils.http.{ HttpMessage, HttpRequest, HttpResponse }

object HttpLimitRequestSinkSemantics {
  object HttpLimitRequestSinkSemanticsOpts {
    val OPT_REQUEST_LIMITS = "request-limits"
    val DEF_REQUEST_LIMITS = 512
    val OPT_REQUEST_RETRY_MAXIMUM_BACKOFF = "request-retry-backoff"
    val DEF_REQUEST_RETRY_MAXIMUM_BACKOFF = Duration(32, SECONDS)

    val OPT_REQUEST_DUMP_FILE_ON_SHUTDOWN = "request-dump-file"
  }
  object HttpOperationStates extends Enumeration {
    type HttpOperationState = Value
    val HTTP_RETRY, HTTP_DONE = Value
  }
  val EVENT_DUMP_CODEC = HttpMessage.charset
}

trait HttpLimitRequestSinkSemantics
  extends HttpSinkSemantics
  with Logging { this: Sink =>
  import HttpLimitRequestSinkSemantics.{
    HttpLimitRequestSinkSemanticsOpts => Opts, _ }
  import HttpOperationStates._

  final private[this] val httpRequests: AtomicLong = new AtomicLong(0)
  /* cfgs are writen once on start inside open */
  final private[this] var cfgHttpRequestLimits: Long = 0
  final private[this] var cfgHttpRequestRetryBackoffMills: Long = _
  final private[this] var cfgHttpRequestRetryMaxBackoff: Double = _
  final private[this] var cfgHttpRequestDumpPath: Option[Path] = None

  /**
   * Internal API - httpResponseFutureHandler
   *
   * Use as future callback of httpRequest, which handle retries and hook in
   * the user defined httpResponseHandler to handle response.
   *
   * Invoke httpComplete when request was considered done.
   */
  final private def httpResponseFutureHandler(request: HttpRequest,
                                              response: Try[HttpResponse],
                                              events: List[Event],
                                              retries: Int,
                                              backoff: Double): Unit =
    httpResponseHandler(events)(response) match {
      case HTTP_DONE =>
        httpComplete()
      case HTTP_RETRY =>
        httpEnqueueRetryRequest(request, response, events,
                                retries - 1, backoff)
    }
  final private def httpResponseFutureHandler(request: HttpRequest,
                                              response: Try[HttpResponse],
                                              events: List[Event],
                                              retries: Int): Unit =
    httpResponseFutureHandler(request, response, events, retries, 0.0)

  /**
   * Internal API - httpEnqueueRetryRequest
   *
   * Handle retries, push HttpMaxRetryException to user defined
   * httpResponseHandler when there is no more retries.
   *
   * Invoke httpComplete when request was considered done.
   */
  final private def httpEnqueueRetryRequest(req: HttpRequest,
                                            response: Try[HttpResponse],
                                            events: List[Event],
                                            retries: Int,
                                            backoff: Double): Unit = {
    if (retries == 0) {   /* no more retries, given up */
      httpResponseHandler(events)(Try({
        response match {
          case Success(r) =>  /* protocol success but application error */
            throw new HttpMaxRetryException("request max retry reached", r)
          case Failure(exc) =>
            throw new HttpMaxRetryException("request max retry reached", exc)
        }
      })) match {
        case HTTP_RETRY =>
          logger.warn("httpResponseHandler return HTTP_RETRY but " +
                      "max retry reached, ignore and discard request")
        case _ =>
          logger.debug("httpResponseHandler return HTTP_DONE but " +
                       "max retry reached, discard request quietly")
      }
      httpComplete()
    } else if (retries < 0 && httpIsClosing &&
               !cfgHttpRequestDumpPath.isEmpty) { /* dump for retry next boot */
      events.foreach(event => {
        logger.debug("sink <{}> prepare for dump event <{}> to disk",
                     getName, event.getBody)
        httpDumpQueue.offer(event)
      })
      httpComplete()
    } else {
      val delay =
        Duration(
          math.min(math.pow(2.0, backoff) * 1000 + Random.nextInt(1000),
                   cfgHttpRequestRetryBackoffMills).toLong,
          MILLISECONDS)
      httpRetry(req, delay).onComplete(response => {
        /* handle retry correctly, to prevent overflow:
         *  - reset to -1
         *  - reset backoff to cfgHttpRequestRetryMaxBackoff */
        httpResponseFutureHandler(req, response, events,
                                  math.max(retries, -1),
                                  math.min(backoff + 1.0,
                                           cfgHttpRequestRetryMaxBackoff))
      })(getHttpExecutionContext)
    }
  }

  /**
   * Low-Level API - HttpCompleteHandler.
   *
   * Handle request completed signal, things we care about in this handler:
   *    - is there any pending commit (<ack>)
   *    - is there any pending close (<close>)
   */
  def httpCompleteHandler(): Unit = {
    val requests = httpRequests.decrementAndGet()
    if (requests < cfgHttpRequestLimits && !transactions.isEmpty) {
      transactions.iterator().asScala
        .foreach({
          case (committer, tran) =>
            logger.debug("sink <{}> acknowledge delayed commit " +
                         "transaction(s), current <{}> open requests",
                         getName, requests)
            ack(committer, tran)
        })
      transactions.clear()
    }

    if (requests == 0) closing.map(closed => close(closed))
  }

  /**
   * High-Level API - HttpEnqueueRequest
   *
   * Not thread safe method, enqueue http request to http processor queue,
   *  - retries <  0, request will be retried infinitely.
   *  - retries == 0, request will not be retried.
   *  - retries >  0, request will be retried maximum <retries> times.
   */
  final def httpEnqueueRequest(req: HttpRequest,
                               events: List[Event],
                               retries: Int): Unit = {
    httpRequests.incrementAndGet()
    httpRequest(req).onComplete(response => {
      /* handle retry correctly, +1 for first time request (normal request) */
      if (retries >= 0) {
        httpResponseFutureHandler(req, response, events, retries + 1)
      } else {
        httpResponseFutureHandler(req, response, events, -1)
      }
    })(getHttpExecutionContext)
  }

  final def httpEnqueueRequest(req: HttpRequest, event: Event): Unit =
    httpEnqueueRequest(req, List(event), 0)
  final def httpEnqueueRequest(req: HttpRequest, events: List[Event]): Unit =
    httpEnqueueRequest(req, events, 0)
  final def httpEnqueueRequest(req: HttpRequest,
                               event: Event,
                               retries: Int): Unit =
    httpEnqueueRequest(req, List(event), retries)

  /**
   * High-Level API - HttpHandleResponse
   *
   * Not thread safe method, user defined callback for handle remote response.
   */
  def httpResponseHandler(
    events: List[Event]): PartialFunction[Try[HttpResponse],
                                          HttpOperationState]

  /**
   * High-Level API - HttpIsClosing
   *
   * Thread safe method, Return true if the HttpLimitRequestSinkSemantics
   * receive a close statement.
   */
  final def httpIsClosing: Boolean = !closing.isEmpty

  override def open(ready: Promise[Ready]): Unit = {
    cfgHttpRequestLimits = getConfiguration.getLong(Opts.OPT_REQUEST_LIMITS,
                                                    Opts.DEF_REQUEST_LIMITS)
    cfgHttpRequestRetryBackoffMills =
      getConfiguration.getDuration(Opts.OPT_REQUEST_RETRY_MAXIMUM_BACKOFF,
                                   Opts.DEF_REQUEST_RETRY_MAXIMUM_BACKOFF)
                      .toMillis
    cfgHttpRequestRetryMaxBackoff = cfgHttpRequestRetryBackoffMills / 1000

    cfgHttpRequestDumpPath =
      getConfiguration.getStringOption(Opts.OPT_REQUEST_DUMP_FILE_ON_SHUTDOWN)
        .map(ps => {
          val p = Paths.get(ps)
          if (!p.isAbsolute()) {
            throw new SinkInitializeException(
              s"cannot initialize http limit request sink with given " +
              s"non absolute dump path, you may want change the setting " +
              s"of <${Opts.OPT_REQUEST_DUMP_FILE_ON_SHUTDOWN}> to an " +
              s"absolute path")
          }

          /**
           * Testing the dump file by trying to create it, and if it is
           * already exists, process and recovery events from it.
           */
          Try { p.toFile().createNewFile() } match {
            case Success(created) =>
              if (created) {
                logger.debug("sink <{}> create empty events dump file " +
                             "<{}> successful during initialize",
                             getName, p.toString)
              } else {
                logger.debug("sink <{}> found events dump file <{}> exists " +
                             "during initialize, trying to resend these " +
                             "events", getName, p.toString)
                Try { httpRecoverEvents(p) } match {
                  case Success(events) =>
                    events.foreach(event => {
                      logger.debug("sink <{}> submit recovered event <{}>",
                                   getName, event.getBody)
                      submit(event)
                    })
                  case Failure(exc) =>
                    throw new SinkInitializeException(
                      s"sink <${getName}> cannot recover events " +
                      s"from disk dump <${p.toString}>, abort initialize", exc)
                }
              }
            case Failure(exc) =>
              throw new SinkInitializeException(
                s"cannot initialize http limit request sink with given non " +
                s"writeable dump path, you may want change the setting of " +
                s"<${Opts.OPT_REQUEST_DUMP_FILE_ON_SHUTDOWN}> to a " +
                s"writeable one", exc)
          }
          /**
           * We done with the dump file, delete it anyway.
           */
          Try { p.toFile().delete() } match {
            case Failure(exc) =>
              throw new SinkInitializeException(
                s"sink <${getName} cannot delete disk dump file after " +
                s"test or process it, got unexcepted exception " +
                s"abort initialize", exc)
            case _ =>
              logger.debug("sink <{}> delete handled disk dump file <{}> " +
                           "during initialization", getName, p.toString)
          }
          p
        })

    super.open(ready)
  }

  final private[this] val transactions =
    JCHashMap.newKeySet[(String, Promise[Transaction])]
  override def ack(committer: String, tran: Promise[Transaction]): Unit = {
    if (httpRequests.get() < cfgHttpRequestLimits) {
      super.ack(committer, tran)
    } else {
      logger.info("sink <{}> got too many (total <{}>) open request " +
                   "exceed request-limits, going to delay transition " +
                   "commit <{}@{}>", getName, httpRequests.get, tran,
                                     tran.hashCode.toHexString)
      transactions.add((committer -> tran))
    }
  }

  /**
   * Dump events to disk (retry next boot), the dump format:
   *  body with headers: base64(body);base64(key),base64(value);base64(key),..
   *  body without header: base64(body)
   *
   * One line for One event.
   */
  final private[this] val httpDumpQueue = new JCLQueue[Event]()
  final private def httpDumpEvents(dumpPath: Path): Int = {
    var dumpLines = 0
    val diskDump =
      new BufferedWriter(
        new OutputStreamWriter(
          new FileOutputStream(dumpPath.toFile()), EVENT_DUMP_CODEC))
    val encoder = Base64.getEncoder()

    while (!httpDumpQueue.isEmpty()) {
      val event = httpDumpQueue.poll()
      /* Get bytes of event body & headers */
      val body = event.getBody.getBytes(EVENT_DUMP_CODEC)
      val headers = event.getHeaders()
        .foldLeft(List[(Array[Byte], Array[Byte])]())({
          case (pairs, (key, value)) =>
            (key.getBytes(EVENT_DUMP_CODEC) ->
             value.getBytes(EVENT_DUMP_CODEC)) :: pairs
        })
      /* Base64 encode and write to disk */
      diskDump.write(encoder.encodeToString(body))
      if (!headers.isEmpty) {
        headers.foreach(pair => {
          diskDump.write(";")
          diskDump.write(encoder.encodeToString(pair._1))
          diskDump.write(",")
          diskDump.write(encoder.encodeToString(pair._2))
        })
      }
      diskDump.newLine()
      dumpLines += 1
    }
    diskDump.close()

    dumpLines
  }

  /**
   * Recover events from disk dump and submit these events, the dump format:
   *  body with headers: base64(body);base64(key),base64(value);base64(key),..
   *  body without header: base64(body)
   *
   * One line for One event.
   */
  final private def httpRecoverEvents(dumpPath: Path): List[Event] = {
    val decoder = Base64.getDecoder()
    scala.io.Source.fromFile(dumpPath.toFile(), EVENT_DUMP_CODEC.name)
      .getLines().foldLeft(List[Event]())((events, content) => {
        val parts = content.split(';')
        val body = new String(decoder.decode(parts.head), EVENT_DUMP_CODEC)
        val headers = parts.tail.foldLeft(Map[String, String]())((hs, p) => {
          val Array(key, value) = p.split(',')
          hs + (new String(decoder.decode(key), EVENT_DUMP_CODEC) ->
                new String(decoder.decode(value), EVENT_DUMP_CODEC))
        })
        SimpleEvent(body, headers) :: events
      })
  }

  @volatile final private[this] var closing: Option[Promise[Closed]] = None
  override def close(closed: Promise[Closed]): Unit =
    if (httpRequests.get() != 0) {
      logger.info("sink <{}> still have <{}> open requests, " +
                  "delay close <{}@{}>", getName, httpRequests.get(), closed,
                                         closed.hashCode.toHexString)
      closing = Some(closed)
    } else {
      if (!httpDumpQueue.isEmpty) {
        try {
          val dumpLines = httpDumpEvents(cfgHttpRequestDumpPath.get)
          logger.info("sink <{}> total dump <{}> events before close",
                      getName, dumpLines)
        } catch {
          case exc: Throwable =>
            logger.error(s"sink <${getName}> cannot dump events to disk, " +
                         s"got unexcepted exception, abort and close", exc)
        }
      }
      super.close(closed)
    }
}

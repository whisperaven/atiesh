/*
 * Copyright (C) Hao Feng
 */

package atiesh.sink

// java
import java.net.URL
import java.util.Base64
import java.nio.charset.Charset
// scala
import scala.util.{ Try, Success, Failure }
// internal
import atiesh.event.Event
import atiesh.utils.{ Configuration, Logging, Compressor }
import atiesh.utils.http.{ HttpMessage, HttpMethods,
                           HttpRequest, HttpResponse }

object HttpSink {
  val SINK_HTTP_METHOD_GET  = "get"
  val SINK_HTTP_METHOD_PUT  = "put"
  val SINK_HTTP_METHOD_POST = "post"
  val VALID_SINK_HTTP_METHODS = List(SINK_HTTP_METHOD_GET,
                                     SINK_HTTP_METHOD_POST,
                                     SINK_HTTP_METHOD_PUT)

  object HttpSinkOpts {
    val OPT_HTTP_METHOD = "http-method"
    val DEF_HTTP_METHOD = SINK_HTTP_METHOD_POST

    val OPT_HTTP_REQ_CODEC = "request-codec"
    val DEF_HTTP_REQ_CODEC = HttpMessage.charsetName

    val OPT_HTTP_CONTENT_TYPE = "http-content-type"
    val DEF_HTTP_CONTENT_TYPE = "application/octet-stream"

    val OPT_MAX_RETRIES      = "max-retries"
    val DEF_MAX_RETRIES: Int = 3 

    val OPT_GZIP_ENABLE  = "gzip-enable"
    val DEF_GZIP_ENABLE  = true
    val OPT_BATCH_ENABLE = "batch-enable"
    val DEF_BATCH_ENABLE = true

    val OPT_BASIC_AUTH_ENABLE   = "basicauth-enable"
    val DEF_BASIC_AUTH_ENABLE   = false
    val OPT_BASIC_AUTH_USERNAME = "basicauth-username"
    val OPT_BASIC_AUTH_PASSWORD = "basicauth-password"

    val OPT_EVENT_QUERY_KEY = "event-query-key"
    val DEF_EVENT_QUERY_KEY = "record"
  }
}

class HttpSink(name: String, dispatcher: String, cfg: Configuration)
  extends AtieshSink(name, dispatcher, cfg)
  with HttpLimitRequestSinkSemantics
  with BatchSinkSemantics
  with Logging {
  import HttpSink.{ HttpSinkOpts => Opts, _ }
  import HttpSinkSemantics.HttpSinkSemanticsOpts
  import HttpLimitRequestSinkSemantics.HttpOperationStates._

  type HttpResponseHandler = PartialFunction[Try[HttpResponse],
                                                 HttpOperationState]

  val cfgHttpRemoteURL =
    new URL(cfg.getString(HttpSinkSemanticsOpts.OPT_REMOTE_URL))
  val cfgHttpMaxRetries = cfg.getInt(Opts.OPT_MAX_RETRIES, Opts.DEF_MAX_RETRIES)

  val cfgHttpReqCodec = {
    val charsetName = cfg.getString(Opts.OPT_HTTP_REQ_CODEC,
                                    Opts.DEF_HTTP_REQ_CODEC)
    try {
      Charset.forName(charsetName)
    } catch {
      case exc: Throwable =>
        throw new SinkInitializeException(
          s"cannot initialize http sink component with given " +
          s"charset <${charsetName}>, which is illegal or unsupported " +
          s"name, you may want change the setting of " +
          s"<${Opts.OPT_HTTP_REQ_CODEC}> to a valid one", exc)
    }
  }
  val cfgHttpGzipEnabled = cfg.getBoolean(Opts.OPT_GZIP_ENABLE,
                                          Opts.DEF_GZIP_ENABLE)
  val cfgHttpBatchEnabled = cfg.getBoolean(Opts.OPT_BATCH_ENABLE,
                                           Opts.DEF_BATCH_ENABLE)
  val cfgHttpBasicAuthEnabled = cfg.getBoolean(Opts.OPT_BASIC_AUTH_ENABLE,
                                               Opts.DEF_BASIC_AUTH_ENABLE)

  val cfgHttpMethod = cfg.getString(Opts.OPT_HTTP_METHOD,
                                    Opts.DEF_HTTP_METHOD).toLowerCase
  if (!VALID_SINK_HTTP_METHODS.contains(cfgHttpMethod)) {
    throw new SinkInitializeException(
      s"cannot initialize http sink ${getName} with http sink method " +
      s"<${cfgHttpMethod}>, should be one of " +
      s"<${VALID_SINK_HTTP_METHODS.mkString("> and <")}>")
  }
  val cfgHttpContentType = cfg.getString(Opts.OPT_HTTP_CONTENT_TYPE,
                                         Opts.DEF_HTTP_CONTENT_TYPE)

  val cfgHttpEventQueryKey = cfg.getString(Opts.OPT_EVENT_QUERY_KEY,
                                           Opts.DEF_EVENT_QUERY_KEY)
  if (cfgHttpMethod == SINK_HTTP_METHOD_GET && cfgHttpEventQueryKey == "") {
    throw new SinkInitializeException(
      s"cannot initialize http sink ${getName} using http " +
      s"<${SINK_HTTP_METHOD_GET}> method with empty " +
      s"<${Opts.OPT_EVENT_QUERY_KEY}>")
  }

  /**
   * - add <Content-Encoding: gzip> when gzip enabled and the push
   *   method is not Http GET (which use query to carry payload).
   * - add <Authorization: Basic b64Auth> when www basic auth
   *   enabled and username/password configured.
   */
  val httpBaseHeaders =
    Map[String, String]("Content-Type" -> cfgHttpContentType) ++ {
      if (cfgHttpGzipEnabled && cfgHttpMethod != SINK_HTTP_METHOD_GET) {
        Map[String, String]("Content-Encoding" -> "gzip")
      } else Map[String, String]()
    } ++ {
      if (cfgHttpBasicAuthEnabled) {
        (for {
          httpAuthUser <- cfg.getStringOption(Opts.OPT_BASIC_AUTH_USERNAME)
          httpAuthPass <- cfg.getStringOption(Opts.OPT_BASIC_AUTH_PASSWORD)
        } yield {
          Base64.getEncoder().encodeToString(
            s"${httpAuthUser}:${httpAuthPass}".getBytes(cfgHttpReqCodec))
        }) match {
          case Some(base64Authorization) =>
            Map[String, String](
              "Authorization" -> s"Basic ${base64Authorization}")
          case _ =>
            throw new SinkInitializeException(
              s"got missing configuration during initializing http sink " +
              s"<${getName}>, either <${Opts.OPT_BASIC_AUTH_USERNAME}> or " +
              s"<${Opts.OPT_BASIC_AUTH_PASSWORD}> not found, can not " +
              s"initialize http authorization string, abort initialize")
        }
      } else Map[String, String]()
    }

  def accept(event: Event): Boolean = true /* accept everything */

  /**
   * Filters for assemble http request body using base events.
   */
  def assembleBody(event: Event): Array[Byte] = HttpMessage.emptyBody
  def assembleBatchBody(events: List[Event], tag: String): Array[Byte] =
    events.foldLeft(List[String]())((es, e) => { e.getBody :: es })
          .mkString("\n")
          .getBytes(cfgHttpReqCodec)

  def process(event: Event): Unit = {
    logger.debug("http sink <{}> drain event <{}>", getName, event.getBody)

    if (cfgHttpBatchEnabled) {
      batchAppend(event)
    } else {
      val (payload, queries) =
        if (cfgHttpMethod == SINK_HTTP_METHOD_GET) {
          (assembleBody(event), List(cfgHttpEventQueryKey -> event.getBody))
        } else {
          (if (cfgHttpGzipEnabled) {
            Compressor.gzipCompress(event.getBody.getBytes(cfgHttpReqCodec))
          } else {
            event.getBody.getBytes(cfgHttpReqCodec)
          }, List[(String, String)]())
        }
      httpSink(event, payload, queries)
    }
  }

  override def process(events: List[Event], tag: String): Unit =
    if (cfgHttpGzipEnabled) {
      httpSink(events, Compressor.gzipCompress(assembleBatchBody(events, tag)), List[(String, String)]())
    } else {
      httpSink(events, assembleBatchBody(events, tag), List[(String, String)]())
    }

  /**
   * Filters for assemble http request queries using base queries.
   */ 
  def assembleQueries(
    event: Event, payload: Array[Byte],
    baseQueries: List[(String, String)]): List[(String, String)] = {
    baseQueries
  }
  def assembleBatchQueries(
    events: List[Event], payload: Array[Byte],
    baseQueries: List[(String, String)]): List[(String, String)] = {
    baseQueries
  }

  /**
   * Filters for assemble http request headers using base headers.
   */ 
  def assembleHeaders(
    event: Event, payload: Array[Byte], queries: List[(String, String)],
    baseHeaders: Map[String, String]): Map[String, String] = {
    baseHeaders
  }
  def assembleBatchHeaders(
    event: List[Event], payload: Array[Byte], queries: List[(String, String)],
    baseHeaders: Map[String, String]): Map[String, String] = {
    baseHeaders
  }

  final private def httpSink(event: Event,
                             payload: Array[Byte],
                             queries: List[(String, String)]): Unit = {
    val qs = assembleQueries(event, payload, queries)
    val hs = assembleHeaders(event, payload, qs, httpBaseHeaders)

    httpSend(List(event), payload, qs, hs)
  }

  final private def httpSink(events: List[Event],
                             payload: Array[Byte],
                             queries: List[(String, String)]): Unit = {
    val qs = assembleBatchQueries(events, payload, queries)
    val hs = assembleBatchHeaders(events, payload, qs, httpBaseHeaders)

    httpSend(events, payload, qs, hs)
  }

  final private def httpSend(events: List[Event],
                             payload: Array[Byte],
                             queries: List[(String, String)],
                             headers: Map[String, String]): Unit = {
    cfgHttpMethod match {
      case SINK_HTTP_METHOD_GET  =>
        httpEnqueueRequest(HttpRequest(cfgHttpRemoteURL.getPath, queries,
                                       HttpMethods.GET, headers,
                                       payload),
                           events, cfgHttpMaxRetries)
      case SINK_HTTP_METHOD_POST =>
        httpEnqueueRequest(HttpRequest(cfgHttpRemoteURL.getPath, queries,
                                       HttpMethods.POST, headers,
                                       payload),
                           events, cfgHttpMaxRetries)
      case SINK_HTTP_METHOD_PUT =>
        httpEnqueueRequest(HttpRequest(cfgHttpRemoteURL.getPath, queries,
                                       HttpMethods.PUT,  headers,
                                       payload),
                           events, cfgHttpMaxRetries)
      case _                => /* just in case, should never happen */
        logger.error(
          "http sink <{}> ignore <{}> event(s) <{}> because " +
          "nonsupported method <{}>",
          getName, events.length, events.headOption.map(_.getBody),
          cfgHttpMethod)
    }
  }

  /** 
   * Response Handler Actions:
   *  -> response with status == 200/201:
   *    -> status 200                      => Done
   *    -> status 201                      => Done
   *  -> response with status != 200/201:
   *    -> status 4xx                      => Done (discard)
   *    -> status 5xx                      => Retry
   *  -> exceptions:
   *    -> HttpMaxRetryException           => Done (discard)
   *    -> Anything Else                   => Retry
   */
  def httpResponseHandler(events: List[Event]): HttpResponseHandler = {
    case Success(response) =>         
      if (response.status == 200 || response.status == 201) {
        logger.debug(
          "http sink <{}> push request for event <{} ({} events)> " +
          "send successed, remote respond content <{}> with code <{}>",
          getName, events.headOption.map(_.getBody).getOrElse("-"),
          events.length, response.stringBody, response.status)
        HTTP_DONE
      } else if (response.status >= 500) {
        logger.warn(
          "http sink <{}> push request for event <{} ({} events)> send " +
          "failed, remote respond content <{}> with code <{}>, retrying",
          getName, events.headOption.map(_.getBody).getOrElse("-"),
          events.length, response.stringBody, response.status)
        HTTP_RETRY
      } else {
        logger.error(
          "http sink <{}> push request for event <{} ({} events)> " +
          "send failed, remote respond content <{}> with code <{}>",
          getName, events.headOption.map(_.getBody).getOrElse("-"),
          events.length, response.stringBody, response.status)
        HTTP_DONE
      }

    case Failure(HttpMaxRetryException(_, Some(exc))) =>
      logger.error(
        s"http sink <${getName}> push request for event " +
        s"<${events.headOption.map(_.getBody).getOrElse("-")} " +
        s"(${events.length} events)> send failed, dropped after retry " +
        s"<${cfgHttpMaxRetries}> times, last try got exception", exc)
      HTTP_DONE

    case Failure(exc) =>
      logger.warn(
        s"http sink <${getName}> push request for event " +
        s"<${events.headOption.map(_.getBody).getOrElse("-")} " +
        s"(${events.length} events)> send failed, retring for low-level " +
        s"unexpected stream exception", exc)
      HTTP_RETRY
  }

  def startup(): Unit = logger.info("starting http sink <{}>", getName)

  def shutdown(): Unit = logger.info("shutting down http sink <{}>", getName)
}

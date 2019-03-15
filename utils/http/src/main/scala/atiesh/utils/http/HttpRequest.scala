package atiesh.utils.http

// scala
import scala.collection.immutable
// java
import java.nio.charset.StandardCharsets
// akka
import akka.http.scaladsl.model.{
  HttpMethod => AkkaHttpMethod,
  HttpMethods => AkkaHttpMethods,
  ContentType => AkkaContentType,
  ContentTypes => AkkaContentTypes,
  HttpRequest => AkkaHttpRequest, _ }
import akka.util.ByteString
// internal
import atiesh.utils.Logging

object HttpMethods extends Enumeration {
  type HttpMethod = Value
  val GET, POST, PUT, DELETE = Value
}
import HttpMethods._

case class HttpRequest(
  uri:     String,
  queries: Seq[(String, String)],
  method:  HttpMethod,
  headers: Map[String, String],
  body:    Array[Byte]) extends Logging {

  /*
   * using this Option, we can remove the Content-Type header from headers and
   *    also make sure the akkaContentType block can access this header value.
   * by doing this, we can prevent the akka WARN:
   *    - Explicitly set HTTP header 'Content-Type: xxx' is ignored,
   *        explicit `Content-Type` header is not allowed.
   *        Set `HttpRequest.entity.contentType` instead.
   */
  var _content_type: Option[String] = None

  val akkaUri = {
    if (queries.isEmpty) Uri(uri)
    else Uri(uri).withQuery(Uri.Query(queries: _*))
  }

  val akkaHttpMethod = {
    if (method == GET) AkkaHttpMethods.GET
    else if (method == POST) AkkaHttpMethods.POST
    else if (method == PUT) AkkaHttpMethods.PUT
    else AkkaHttpMethods.DELETE
  }

  val akkaHttpHeaders = headers.foldLeft(immutable.Seq[HttpHeader]())({
    case (hs, (k, v)) =>
      if (k.toLowerCase == "content-type") {
        _content_type = Some(v.toLowerCase)
        hs
      } else {
        import HttpHeader.ParsingResult._
        HttpHeader.parse(k, v) match {
          case Ok(h, errs) =>
            if (errs.length != 0 ) {
              val errstr = errs.foldLeft("")((summary, ei) => { summary + ei.format(true) })
              logger.warn("got error during http header parse for outgoing request {}, raw header was <{}>", errstr, h)
            }
            hs :+ h

          case Error(err) =>
            logger.error("ignore bad http header of outgoing request, {}", err.format(true))
            hs
        }
      }
  })

  val akkaContentType = {
    if (_content_type.isEmpty) {
      _content_type = headers
        .find({ case (k, v) => k.toLowerCase == "content-type" })
        .map({ case (_, t) => t.toLowerCase })
    }

    _content_type.map(contentType => {
      contentType match {
        case c if c == "application/json" => AkkaContentTypes.`application/json`
        case c if c == "application/octet-stream" => AkkaContentTypes.`application/octet-stream`
        case c if c == "application/grpc+proto" => AkkaContentTypes.`application/grpc+proto`
        case c if c == "text/plain(UTF-8)" => AkkaContentTypes.`text/plain(UTF-8)`
        case c if c == "text/html(UTF-8)" => AkkaContentTypes.`text/html(UTF-8)`
        case c if c == "text/xml(UTF-8)" => AkkaContentTypes.`text/xml(UTF-8)`
        case c if c == "text/csv(UTF-8)" => AkkaContentTypes.`text/csv(UTF-8)`
        case _ => AkkaContentTypes.NoContentType
      }
    }).getOrElse(AkkaContentTypes.NoContentType)
  }

  val akkaHttpEntity = {
    if (body.isEmpty) HttpEntity.Empty
    else HttpEntity(akkaContentType, ByteString(body))
  }
}

object HttpRequest {
  val charset: String = StandardCharsets.UTF_8.name()

  val emptyBody   = Array[Byte]()
  val emptyQuery  = List[(String, String)]()
  val emptyHeader = Map[String, String]()

  /* queries as Map */
  def apply(
    uri:     String,
    queries: Map[String, String],
    method:  HttpMethod,
    headers: Map[String, String],
    body:    Array[Byte]): HttpRequest = HttpRequest(uri, queries.toList, method, headers, body)

  /* no body content - GET without queres & headers */
  def apply(uri: String): HttpRequest = HttpRequest(uri, emptyQuery, GET, emptyHeader, emptyBody)

  /* string as body - request without queries */
  def apply(
    uri: String,
    method: HttpMethod,
    headers: Map[String, String],
    body: String): HttpRequest = HttpRequest(uri, emptyQuery, method, headers, body.getBytes(charset))

  /* string as body - request without headers */
  def apply(
    uri: String,
    quires: Map[String, String],
    method: HttpMethod,
    body: String): HttpRequest = HttpRequest(uri, quires, method, emptyHeader, body.getBytes(charset))

  def apply(
    uri: String,
    quires: Seq[(String, String)],
    method: HttpMethod,
    body: String): HttpRequest = HttpRequest(uri, quires, method, emptyHeader, body.getBytes(charset))

  /* string as body - request without queries and headers */
  def apply(
    uri: String,
    method: HttpMethod,
    body: String): HttpRequest = HttpRequest(uri, emptyQuery, method, emptyHeader, body.getBytes(charset))

  /* bytes as body - request without queries */
  def apply(
    uri: String,
    method: HttpMethod,
    headers: Map[String, String],
    body: Array[Byte]): HttpRequest = HttpRequest(uri, emptyQuery, method, headers, body)

  /* bytes as body - request without headers */
  def apply(
    uri: String,
    quires: Map[String, String],
    method: HttpMethod,
    body: Array[Byte]): HttpRequest = HttpRequest(uri, quires, method, emptyHeader, body)

  def apply(
    uri: String,
    quires: Seq[(String, String)],
    method: HttpMethod,
    body: Array[Byte]): HttpRequest = HttpRequest(uri, quires, method, emptyHeader, body)

  /* bytes as body - request without queries and headers */
  def apply(
    uri: String,
    method: HttpMethod,
    body: Array[Byte]): HttpRequest = HttpRequest(uri, emptyQuery, method, emptyHeader, body)
}

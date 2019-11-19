/*
 * Copyright (C) Hao Feng
 */

package atiesh.utils.http

// java
import java.nio.charset.{ Charset, StandardCharsets }
// scala
import scala.collection.immutable
import scala.language.implicitConversions
// akka
import akka.http.scaladsl.model.{ Uri => AkkaUri,
                                  StatusCode => AkkaStatusCode,
                                  HttpHeader => AkkaHttpHeader,
                                  ContentType => AkkaContentType,
                                  ContentTypes => AkkaContentTypes,
                                  HttpResponse => AkkaHttpResponse }
// internal
import atiesh.utils.Logging

object HttpMessage extends Logging {
  object implicits {
    implicit def asStringBody(body: Array[Byte]): String = toStringBody(body)
  }

  val charset: Charset = StandardCharsets.UTF_8
  val charsetName: String = StandardCharsets.UTF_8.name()

  val defaultProtocol = HttpProtocols.HTTP_PROTOCOL_10
  val defaultContentType = AkkaContentTypes.NoContentType

  val contentTypeMapper = Map[String, AkkaContentType](
    "application/json"         -> AkkaContentTypes.`application/json`,
    "application/grpc+proto"   -> AkkaContentTypes.`application/grpc+proto`,
    "application/octet-stream" -> AkkaContentTypes.`application/octet-stream`,
    "text/plain(utf-8)"        -> AkkaContentTypes.`text/plain(UTF-8)`,
    "text/html(utf-8)"         -> AkkaContentTypes.`text/html(UTF-8)`,
    "text/xml(utf-8)"          -> AkkaContentTypes.`text/xml(UTF-8)`,
    "text/csv(utf-8)"          -> AkkaContentTypes.`text/csv(UTF-8)`
  )
  val akkaContentTypeMapper: Map[AkkaContentType, String] =
    contentTypeMapper.map(_.swap)

  val emptyBody   = Array[Byte]()
  val emptyQuery  = List[(String, String)]()
  val emptyHeader = Map[String, String]()

  def toStringBody(bodyBytes: Array[Byte]): String =
    new String(bodyBytes, charset)
  def toStringBody(bodyBytes: Array[Byte], charset: Charset): String =
    new String(bodyBytes, charset)
  def toStringBody(bodyBytes: Array[Byte], charset: String): String =
    new String(bodyBytes, charset)

  // status code parser
  def parseStatus(status: Int): AkkaStatusCode = {
    try { AkkaStatusCode.int2StatusCode(status) }
    catch {
      case exc: Throwable =>
        throw new HttpNonStandardStatusCodeException(
          s"unsupported non-standard status code ${status}", exc)
    }
  }

  // uri parser
  def parseUri(queries: Seq[(String, String)], uri: String): AkkaUri = {
    if (queries.isEmpty) AkkaUri(uri)
    else AkkaUri(uri).withQuery(AkkaUri.Query(queries: _*))
  }

  def parseUri(u: AkkaUri): (List[(String, String)], String) = {
    val seqQs = u.queryString(charset)
                  .map(_.split("&"))
                  .getOrElse(Array[String]())
                  .foldLeft(List[(String, String)]())((qs, q) => {
                    val p = q.split("=")
                    if (p.length == 2) (p.head, p.last) :: qs
                    else (p.head, "") :: qs
                  }).reverse
    val uri = u match {
      case AkkaUri.Path(path) => path
    }
    (seqQs, uri)
  }

  // header parser
  import AkkaHttpHeader.ParsingResult._
  def parseHeaders(
    headers: Map[String, String]): immutable.Seq[AkkaHttpHeader] =
    headers.foldLeft(immutable.Seq[AkkaHttpHeader]())({
      case (hs, (k, v)) =>
        if (k.toLowerCase != "content-type") {
          AkkaHttpHeader.parse(k, v) match {
            case Ok(h, errs) =>
              if (errs.length != 0 ) {
                val errstr =
                  errs.foldLeft("")((summary, ei) => {
                    summary + ei.format(true)
                  })
                logger.warn("got error during http header parse for " +
                            "outgoing request {}, raw header was <{}>",
                            errstr, h)
              }
              hs :+ h

            case Error(err) =>
              logger.error("ignore bad http header of outgoing " +
                           "request, {}", err.format(true))
              hs
          }
        } else hs
    })

  def parseHeaders(headers: Seq[AkkaHttpHeader]): Map[String, String] =
    headers.foldLeft(Map[String, String]())((hs, header) => {
      header match {
        case AkkaHttpHeader(key, value) => hs + (key -> value)
      }
    })

  def parseHeaders(response: AkkaHttpResponse): Map[String, String] =
    response.headers.foldLeft(
      akkaContentTypeMapper.get(response.entity.contentType)
        .map(contentType => Map("content-type" -> contentType))
        .getOrElse(Map[String, String]()))(
      (hs, h) => { hs + (h.name -> h.value) }
    )

  // content type parser
  def parseContentType(contentType: String): AkkaContentType =
    contentTypeMapper.getOrElse(contentType, defaultContentType)
}

/*
 * Copyright (C) Hao Feng
 */

package atiesh.utils.http

// scala
import scala.language.implicitConversions
// akka
import akka.http.scaladsl.model.{ StatusCode,
                                  HttpEntity => AkkaHttpEntity,
                                  HttpResponse => AkkaHttpResponse,
                                  ContentTypes => AkkaContentTypes }
import akka.util.ByteString

object HttpResponse {
  import HttpMessage._

  object implicits {
    implicit def asAkkaResponse(resp: HttpResponse): AkkaHttpResponse =
      resp.akkaHttpResponse
  }

  def apply(
    status: Int,
    headers: Map[String, String],
    body: String): HttpResponse =
    HttpResponse(status, headers, ByteString(body, charset))

  def apply(
    status: Int,
    headers: Map[String, String],
    body: Array[Byte]): HttpResponse =
    HttpResponse(status, headers, ByteString(body))
}

/**
 * Atiesh http response representation, a simple wrapper for akka-http.
 */
final case class HttpResponse(val status: Int,
                              val headers: Map[String, String],
                              val body: ByteString) {
  import HttpMessage._

  lazy val stringBody = body.decodeString(charset)

  lazy val akkaHttpStatus = parseStatus(status)
  lazy val akkaHttpHeaders = parseHeaders(headers)

  lazy val akkaContentType = headers
    .find({ case (k, v) => k.toLowerCase == "content-type" })
    .map({ case (_, t) => t.toLowerCase })
    .map(c => parseContentType(c))
    .getOrElse(AkkaContentTypes.NoContentType)

  lazy val akkaHttpEntity = {
    if (body.isEmpty) AkkaHttpEntity.Empty
    else AkkaHttpEntity(akkaContentType, body)
  }

  def akkaHttpResponse: AkkaHttpResponse = AkkaHttpResponse(akkaHttpStatus,
                                                            akkaHttpHeaders,
                                                            akkaHttpEntity)
}

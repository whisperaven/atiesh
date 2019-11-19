/*
 * Copyright (C) Hao Feng
 */

package atiesh.utils.http

// scala
import scala.language.implicitConversions
// akka
import akka.http.scaladsl.model.{ HttpEntity => AkkaHttpEntity,
                                  HttpRequest => AkkaHttpRequest,
                                  ContentTypes => AkkaContentTypes }
import akka.util.ByteString
// internal
import atiesh.utils.Logging

object HttpRequest extends Logging {
  import HttpMessage._

  object implicits {
    implicit def asAkkaRequest(req: HttpRequest): AkkaHttpRequest =
      req.akkaHttpRequest
  }

  /* bytes body with full args - queries as List */
  def apply(
    uri:     String,
    queries: Seq[(String, String)],
    method:  HttpMethods.HttpMethod,
    headers: Map[String, String],
    body:    Array[Byte]): HttpRequest =
    HttpRequest(uri, queries, method, headers, ByteString(body))

  /* bytes body with full args - queries as Map */
  def apply(
    uri:     String,
    queries: Map[String, String],
    method:  HttpMethods.HttpMethod,
    headers: Map[String, String],
    body:    Array[Byte]): HttpRequest =
    HttpRequest(uri, queries.toList, method, headers, ByteString(body))

  /* simplest GET - without queres & headers */
  def apply(uri: String): HttpRequest =
    HttpRequest(
      uri, emptyQuery, HttpMethods.GET, emptyHeader, ByteString.empty)

  /* request without queries and headers */
  def apply(
    uri: String,
    method: HttpMethods.HttpMethod,
    body: String): HttpRequest =
    HttpRequest(
      uri, emptyQuery, method, emptyHeader, ByteString(body, charset))
  def apply(
    uri: String,
    method: HttpMethods.HttpMethod,
    body: Array[Byte]): HttpRequest =
    HttpRequest(uri, emptyQuery, method, emptyHeader, ByteString(body))

  /* request without queries */
  def apply(
    uri: String,
    method: HttpMethods.HttpMethod,
    headers: Map[String, String],
    body: String): HttpRequest =
    HttpRequest(
      uri, emptyQuery, method, headers, ByteString(body, charset))
  def apply(
    uri: String,
    method: HttpMethods.HttpMethod,
    headers: Map[String, String],
    body: Array[Byte]): HttpRequest =
    HttpRequest(uri, emptyQuery, method, headers, ByteString(body))

  /* request without headers: queries as Map / queries as Seq */
  def apply(
    uri: String,
    queries: Map[String, String],
    method: HttpMethods.HttpMethod,
    body: String): HttpRequest =
    HttpRequest(
      uri, queries.toList, method, emptyHeader, ByteString(body, charset))
  def apply(
    uri: String,
    queries: Seq[(String, String)],
    method: HttpMethods.HttpMethod,
    body: String): HttpRequest =
    HttpRequest(
      uri, queries, method, emptyHeader, ByteString(body, charset))
  def apply(
    uri: String,
    queries: Map[String, String],
    method: HttpMethods.HttpMethod,
    body: Array[Byte]): HttpRequest =
    HttpRequest(uri, queries.toList, method, emptyHeader, ByteString(body))
  def apply(
    uri: String,
    queries: Seq[(String, String)],
    method: HttpMethods.HttpMethod,
    body: Array[Byte]): HttpRequest =
    HttpRequest(uri, queries, method, emptyHeader, ByteString(body))
}

/**
 * Atiesh http request representation, a wrapper for akka-http.
 */
final case class HttpRequest(val uri:     String,
                             val queries: Seq[(String, String)],
                             val method:  HttpMethods.HttpMethod,
                             val headers: Map[String, String],
                             val body:    ByteString) {
  import HttpMessage._

  lazy val stringBody = body.decodeString(charset)

  val akkaHttpMethod = method

  lazy val akkaUri = parseUri(queries, uri)
  lazy val akkaHttpHeaders = parseHeaders(headers)

  /*
   * We should remove the Content-Type header from headers and also make
   * sure the akkaContentType block can access the Content-Type header value.
   *
   * By doing this, we can prevent the akka WARN:
   *  - Explicitly set HTTP header 'Content-Type: xxx' is ignored,
   *      explicit `Content-Type` header is not allowed.
   *      Set `HttpRequest.entity.contentType` instead.
   */
  lazy val akkaContentType = headers
    .find({ case (k, v) => k.toLowerCase == "content-type" })
    .map({ case (_, t) => t.toLowerCase })
    .map(c => parseContentType(c))
    .getOrElse(AkkaContentTypes.NoContentType)

  lazy val akkaHttpEntity = {
    if (body.isEmpty) AkkaHttpEntity.Empty
    else AkkaHttpEntity(akkaContentType, body)
  }

  lazy val akkaHttpRequest: AkkaHttpRequest = AkkaHttpRequest(akkaHttpMethod,
                                                              akkaUri,
                                                              akkaHttpHeaders,
                                                              akkaHttpEntity)
}

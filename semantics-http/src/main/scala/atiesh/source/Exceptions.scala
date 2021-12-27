/*
 * Copyright (C) Hao Feng
 */

package atiesh.source

// internal
import atiesh.utils.http.HttpRequest

object HttpTooManyRequestException {
  def unapply(exc: HttpTooManyRequestException): Option[(Option[HttpRequest], Option[Throwable])] = {
    exc match {
      case e: HttpTooManyRequestException => Some(e.request, e.cause)
      case _ => None
    }
  }
}

class HttpTooManyRequestException(val message: String,
                                  val request: Option[HttpRequest],
                                  val cause: Option[Throwable]) extends RuntimeException(message, cause.getOrElse(null)) {
  def this(message: String) = this(message, None, None)
  def this(message: String, cause: Throwable) = this(message, None, Some(cause))
  def this(message: String, request: HttpRequest) = this(message, Some(request), None)
  def this(message: String, request: HttpRequest, cause: Throwable) = this(message, Some(request), Some(cause))
  def this(cause: Throwable) = this(null, None, Some(cause))
  def this() = this(null, None, None)
}

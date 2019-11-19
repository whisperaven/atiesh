/*
 * Copyright (C) Hao Feng
 */

package atiesh.sink

// internal
import atiesh.utils.http.HttpResponse

object HttpMaxRetryException {
  def unapply(exc: HttpMaxRetryException): Option[(Option[HttpResponse], Option[Throwable])] = {
    exc match {
      case e: HttpMaxRetryException => Some(e.response, e.cause)
      case _ => None
    }
  }
}

class HttpMaxRetryException(val message: String,
                            val response: Option[HttpResponse],
                            val cause: Option[Throwable]) extends RuntimeException(message, cause.getOrElse(null)) {
  def this(message: String) = this(message, None, None)
  def this(message: String, cause: Throwable) = this(message, None, Some(cause))
  def this(message: String, response: HttpResponse) = this(message, Some(response), None)
  def this(message: String, response: HttpResponse, cause: Throwable) = this(message, Some(response), Some(cause))
  def this(cause: Throwable) = this("", None, Some(cause))
  def this() = this("", None, None)
}

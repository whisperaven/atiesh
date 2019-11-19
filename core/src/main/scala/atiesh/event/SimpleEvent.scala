/*
 * Copyright (C) Hao Feng
 */

package atiesh.event

object SimpleEvent {
  def apply(payload: String, headers: Map[String, String]): Event =
    new SimpleEvent(payload, headers)
}

/**
 * Atiesh simplest event represent.
 */
class SimpleEvent(payload: String, headers: Map[String, String])
  extends AtieshEvent(payload, headers) {
  def setBody(body: String): Event =
    SimpleEvent(body, headers)
  def setHeaders(headers: Map[String, String]): Event =
    SimpleEvent(payload, headers)
  def setHeaders(pairs: (String, String)*): Event =
    SimpleEvent(
      payload,
      pairs.foldLeft(getHeaders())({ case (m, (k, v)) => m + (k -> v) }))
}

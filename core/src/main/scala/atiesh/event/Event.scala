/*
 * Copyright (C) Hao Feng
 */

package atiesh.event

/**
 * Atiesh event interface.
 */
trait Event {
  val payload: String
  val headers: Map[String, String]

  def getBody(): String
  def setBody(body: String): Event

  def getHeaders(): Map[String, String]
  def setHeaders(headers: Map[String, String]): Event
  def setHeaders(pairs: (String, String)*): Event
}

/**
 * Atiesh empty event singleton.
 */
final case object Empty extends Event {
  val payload: String = ""
  val headers: Map[String, String] = Map[String, String]()

  def getBody(): String = ""
  def setBody(body: String): Event = Empty

  def getHeaders(): Map[String, String] = headers
  def setHeaders(headers: Map[String, String]): Event = Empty
  def setHeaders(pairs: (String, String)*): Event = Empty
}

/**
 * Atiesh abstract event (an event interface with constructor).
 */
abstract class AtieshEvent(val payload: String,
                           val headers: Map[String, String])
  extends Event {
  def getBody(): String = payload
  def getHeaders(): Map[String, String] = headers

  def setBody(body: String): Event
  def setHeaders(headers: Map[String, String]): Event
  def setHeaders(pairs: (String, String)*): Event
}

/*
 * Copyright (C) Hao Feng
 */

package atiesh.sink

// internal
import atiesh.event.Event
import atiesh.utils.{ Configuration, Logging }

class DevNull(name: String, dispatcher: String, cfg: Configuration) extends AtieshSink(name, dispatcher, cfg) with Logging {
  def accept(event: Event): Boolean = true
  def process(event: Event): Unit = logger.debug("discard event <{}> by sink <{}>", event.getBody, getName)
  def process(sig: Int): Unit = logger.debug("got signal <{}> by sink <{}>", sig, getName)

  def shutdown(): Unit = logger.info("closing sink <{}>", getName)
  def startup(): Unit = logger.info("starting sink <{}>", getName)
}

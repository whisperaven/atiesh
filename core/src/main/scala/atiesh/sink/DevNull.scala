/*
 * Copyright (C) Hao Feng
 */

package atiesh.sink

// internal
import atiesh.event.Event
import atiesh.utils.{ Configuration, Logging }

/**
 * Atiesh builtin sink component, log and discard everything.
 */
class DevNull(name: String, dispatcher: String, cfg: Configuration)
  extends AtieshSink(name, dispatcher, cfg)
  with Logging {
  def accept(event: Event): Boolean = true
  def process(event: Event): Unit =
    logger.debug("discard event <{}> by sink <{}>", event.getBody, getName)

  def startup(): Unit = logger.info("starting sink <{}>", getName)
  def shutdown(): Unit = logger.info("closing sink <{}>", getName)
}

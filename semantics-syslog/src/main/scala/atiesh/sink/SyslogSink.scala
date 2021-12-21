/*
 * Copyright (C) Hao Feng
 */

package atiesh.sink

// scala
import scala.util.{ Success, Failure }
// internal 
import atiesh.event.Event
import atiesh.utils.{ Configuration, Logging }

/**
 * Default Syslog sink, implement on top of the Syslog Semantics.
 */
class SyslogSink(name: String, dispatcher: String, cfg: Configuration)
  extends AtieshSink(name, dispatcher, cfg)
  with SyslogSinkSemantics
  with Logging {

  // default impl, accept everything
  def accept(event: Event): Boolean = true

  def process(event: Event): Unit = {
    logger.debug("syslog sink <{}> drain event <{}>", getName, event.getBody)
    syslogSendMessage(event.getBody) match {
      case Failure(exc) =>
        logger.error(s"sink <${getName}> send message <${event.getBody}> " +
                     s"to remote syslog server failed", exc)
      case _ =>
        logger.debug("sink <{}> send message <{}> to remote syslog " +
                     "server successed", getName, event.getBody)
    }
  }

  def startup(): Unit = logger.info("starting sink <{}>", getName)
  def shutdown(): Unit = logger.info("shutting down sink <{}>", getName)
}

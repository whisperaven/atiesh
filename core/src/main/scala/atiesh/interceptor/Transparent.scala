/*
 * Copyright (C) Hao Feng
 */

package atiesh.interceptor

// internal
import atiesh.event.Event
import atiesh.utils.{ Configuration, Logging }

class Transparent(name: String, priority: Int, cfg: Configuration) extends AtieshInterceptor(name, priority, cfg) with Logging {
  def intercept(event: Event): Event = {
    logger.debug("passing through event {} by interceptor {}", event.getBody, getName)
    event
  }
}

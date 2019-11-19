/*
 * Copyright (C) Hao Feng
 */

package atiesh.interceptor

// internal
import atiesh.event.Event
import atiesh.utils.{ Configuration, Logging }

/**
 * Atiesh builtin interceptor component, log and pass through everything.
 */
class Transparent(name: String, priority: Int, cfg: Configuration)
  extends AtieshInterceptor(name, priority, cfg)
  with Logging {
  def intercept(event: Event): Event = {
    logger.debug("passing through event {} by interceptor {}",
                 event.getBody, getName)

    metricsInterceptorEventInterceptCounter.increment()
    metricsInterceptorComponentEventInterceptCounter.increment()

    event
  }
}

/*
 * Copyright (C) Hao Feng
 */

package atiesh.interceptor

// internal
import atiesh.event.{ Event, Empty }
import atiesh.utils.{ Configuration, Logging }

/**
 * Atiesh builtin interceptor component, log and discard everything.
 */
class DevNull(name: String, priority: Int, cfg: Configuration)
  extends AtieshInterceptor(name, priority, cfg)
  with Logging {
  def intercept(event: Event): Event = {
    logger.debug("discard event {} by interceptor {}",
                 event.getBody, getName)

    metricsInterceptorEventInterceptCounter.increment()
    metricsInterceptorEventDiscardCounter.increment()

    metricsInterceptorComponentEventInterceptCounter.increment()
    metricsInterceptorComponentEventDiscardCounter.increment()

    Empty
  }
}

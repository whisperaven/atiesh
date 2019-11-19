/*
 * Copyright (C) Hao Feng
 */

package atiesh.interceptor

// internal
import atiesh.metrics.{ Metrics, ComponentMetrics }

object InterceptorMetrics {
  // event intercept metric tags
  val metricsInterceptorEventInterceptTag = ("type" -> "intercept_events")
  val metricsInterceptorEventDiscardTag = ("type" -> "intercept_discarded")
}

/**
 * Atiesh interceptor component builtin metrics.
 */
trait InterceptorMetrics extends ComponentMetrics { this: Interceptor =>
  import InterceptorMetrics._

  // event intercepted root counter
  final lazy val metricsInterceptorEventInterceptedCounter: Metrics.BaseCounter =
    Metrics.newCounter("interceptor_event_intercepted", Metrics.emptyMeasurementUnit)

  // event intercepted counter
  final lazy val metricsInterceptorEventInterceptCounter: Metrics.Counter =
    Metrics.newCounter(metricsInterceptorEventInterceptedCounter,
                       metricsComponentTypeTag + metricsInterceptorEventInterceptTag)
  final lazy val metricsInterceptorComponentEventInterceptCounter: Metrics.Counter =
    Metrics.newCounter(metricsInterceptorEventInterceptedCounter,
                       metricsComponentTags + metricsInterceptorEventInterceptTag)

  // event intercept discarded counter
  final lazy val metricsInterceptorEventDiscardCounter: Metrics.Counter =
    Metrics.newCounter(metricsInterceptorEventInterceptedCounter,
                       metricsComponentTypeTag + metricsInterceptorEventDiscardTag)
  final lazy val metricsInterceptorComponentEventDiscardCounter: Metrics.Counter =
    Metrics.newCounter(metricsInterceptorEventInterceptedCounter,
                       metricsComponentTags + metricsInterceptorEventDiscardTag)
}

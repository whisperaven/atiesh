/*
 * Copyright (C) Hao Feng
 */

package atiesh.sink

// internal
import atiesh.component.Component
import atiesh.metrics.{ Metrics, ComponentMetrics }

object AliyunSLSSinkMetrics {
  // aliyun sls sink publish counter tags
  final val metricsSLSSinkEventPublishSuccessTag = ("type" -> "publish_succeeded")
  final val metricsSLSSinkEventPublishFailureTag = ("type" -> "publish_failed")
}

/**
 * Atiesh aliyun sls sink semantics builtin metrics.
 */
trait AliyunSLSSinkMetrics { this: SinkMetrics =>
  import AliyunSLSSinkMetrics._

  // event publish root counter
  final lazy val metricsSLSSinkEventPublishCounter: Metrics.BaseCounter =
    Metrics.newCounter("sink_sls_publish", Metrics.emptyMeasurementUnit)

  // publish counter for successded
  final lazy val metricsSLSSinkEventPublishSuccessCounter: Metrics.Counter =
    Metrics.newCounter(metricsSLSSinkEventPublishCounter,
                       metricsComponentTypeTag + metricsSLSSinkEventPublishSuccessTag)
  final lazy val metricsSLSSinkComponentEventPublishSuccessCounter: Metrics.Counter =
    Metrics.newCounter(metricsSLSSinkEventPublishCounter,
                       metricsComponentTags + metricsSLSSinkEventPublishSuccessTag)

  // publish counter for failed
  final lazy val metricsSLSSinkEventPublishFailureCounter: Metrics.Counter =
    Metrics.newCounter(metricsSLSSinkEventPublishCounter,
                       metricsComponentTypeTag + metricsSLSSinkEventPublishFailureTag)
  final lazy val metricsSLSSinkComponentEventPublishFailureCounter: Metrics.Counter =
    Metrics.newCounter(metricsSLSSinkEventPublishCounter,
                       metricsComponentTags + metricsSLSSinkEventPublishFailureTag)
}

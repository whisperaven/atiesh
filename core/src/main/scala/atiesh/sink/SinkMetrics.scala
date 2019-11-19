/*
 * Copyright (C) Hao Feng
 */

package atiesh.sink

// internal
import atiesh.component.Component
import atiesh.metrics.{ Metrics, ComponentMetrics }

/**
 * Atiesh sink component builtin metrics.
 */
trait SinkMetrics extends ComponentMetrics { this: SinkSemantics with Component =>
  final lazy val metricsSinkEventAcceptedCounter: Metrics.Counter =
    Metrics.newCounter("sink_event_accepted", Metrics.emptyMeasurementUnit, metricsComponentTypeTag)
  final lazy val metricsSinkComponentEventAcceptedCounter: Metrics.Counter =
    Metrics.newCounter("sink_event_accepted", Metrics.emptyMeasurementUnit, metricsComponentTags)

  final lazy val metricsSinkBatchAcceptedCounter: Metrics.Counter =
    Metrics.newCounter("sink_batch_accepted", Metrics.emptyMeasurementUnit, metricsComponentTypeTag)
  final lazy val metricsSinkComponentBatchAcceptedCounter: Metrics.Counter =
    Metrics.newCounter("sink_batch_accepted", Metrics.emptyMeasurementUnit, metricsComponentTags)

  final lazy val metricsSinkSignalAcceptedCounter: Metrics.Counter =
    Metrics.newCounter("sink_signal_accepted", Metrics.emptyMeasurementUnit, metricsComponentTypeTag)
  final lazy val metricsSinkComponentSignalAcceptedCounter: Metrics.Counter =
    Metrics.newCounter("sink_signal_accepted", Metrics.emptyMeasurementUnit, metricsComponentTags)
}

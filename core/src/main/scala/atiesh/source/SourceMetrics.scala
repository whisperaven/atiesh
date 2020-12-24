/*
 * Copyright (C) Hao Feng
 */

package atiesh.source

// internal
import atiesh.component.Component
import atiesh.metrics.{ Metrics, ComponentMetrics }

object SourceMetrics {
  // event intercepted metric tags
  val metricsSourceEventInterceptErrorTag = ("type" -> "intercept_errors")
  val metricsSourceEventInterceptDiscardTag = ("type" -> "intercept_discarded")
  // event sibmitted metric tags
  val metricsSourceEventSubmitDiscardTag = ("type" -> "submit_discarded")
}

/**
 * Atiesh source component builtin metrics.
 */
trait SourceMetrics extends ComponentMetrics { this: SourceSemantics with Component =>
  import SourceMetrics._

  // source main cycle runs counter
  final lazy val metricsSourceCycleRunCounter: Metrics.Counter =
    Metrics.newCounter("source_cycle_runs", Metrics.emptyMeasurementUnit, metricsComponentTypeTag)
  final lazy val metricsSourceComponentCycleRunCounter: Metrics.Counter =
    Metrics.newCounter("source_cycle_runs", Metrics.emptyMeasurementUnit, metricsComponentTags)
  // source main cycle errors counter
  final lazy val metricsSourceCycleErrorCounter: Metrics.Counter =
    Metrics.newCounter("source_cycle_errors", Metrics.emptyMeasurementUnit, metricsComponentTypeTag)
  final lazy val metricsSourceComponentCycleErrorCounter: Metrics.Counter =
    Metrics.newCounter("source_cycle_errors", Metrics.emptyMeasurementUnit, metricsComponentTags)

  // event accepted counter
  final lazy val metricsSourceEventAcceptedCounter: Metrics.Counter =
    Metrics.newCounter("source_event_accepted", Metrics.emptyMeasurementUnit, metricsComponentTypeTag)
  final lazy val metricsSourceComponentEventAcceptedCounter: Metrics.Counter =
    Metrics.newCounter("source_event_accepted", Metrics.emptyMeasurementUnit, metricsComponentTags)

  // event intercepted root counter
  final lazy val metricsSourceEventInterceptedCounter: Metrics.BaseCounter =
    Metrics.newCounter("source_event_intercepted", Metrics.emptyMeasurementUnit)

  // event intercept exceptions counter
  final lazy val metricsSourceEventInterceptErrorCounter: Metrics.Counter =
    Metrics.newCounter(metricsSourceEventInterceptedCounter,
                       metricsComponentTypeTag + metricsSourceEventInterceptErrorTag)
  final lazy val metricsSourceComponentEventInterceptErrorCounter: Metrics.Counter =
    Metrics.newCounter(metricsSourceEventInterceptedCounter,
                       metricsComponentTags + metricsSourceEventInterceptErrorTag)

  // event intercept discard counter
  final lazy val metricsSourceEventInterceptDiscardCounter: Metrics.Counter =
    Metrics.newCounter(metricsSourceEventInterceptedCounter,
                       metricsComponentTypeTag + metricsSourceEventInterceptDiscardTag)
  final lazy val metricsSourceComponentEventInterceptDiscardCounter: Metrics.Counter =
    Metrics.newCounter(metricsSourceEventInterceptedCounter,
                       metricsComponentTags + metricsSourceEventInterceptDiscardTag)

  // event sibmited root counter
  final lazy val metricsSourceEventSubmittedCounter: Metrics.BaseCounter =
    Metrics.newCounter("source_event_submitted", Metrics.emptyMeasurementUnit)

  // event sibmit with no sink accept
  final lazy val metricsSourceEventSubmitDiscardCounter: Metrics.Counter =
    Metrics.newCounter(metricsSourceEventSubmittedCounter,
                       metricsComponentTypeTag + metricsSourceEventSubmitDiscardTag)
  final lazy val metricsSourceComponentEventSubmitDiscardCounter: Metrics.Counter =
    Metrics.newCounter(metricsSourceEventSubmittedCounter,
                       metricsComponentTags + metricsSourceEventSubmitDiscardTag)

  // signal handles
  final lazy val metricsSourceSignalAcceptedCounter: Metrics.Counter =
    Metrics.newCounter("source_signal_accepted", Metrics.emptyMeasurementUnit, metricsComponentTypeTag)
  final lazy val metricsSourceComponentSignalAcceptedCounter: Metrics.Counter =
    Metrics.newCounter("source_signal_accepted", Metrics.emptyMeasurementUnit, metricsComponentTags)
}

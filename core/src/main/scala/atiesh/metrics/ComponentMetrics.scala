/*
 * Copyright (C) Hao Feng
 */

package atiesh.metrics

// internal
import atiesh.component.Component

/**
 * Atiesh component builtin metrics.
 */
trait ComponentMetrics { this: Component =>
  final lazy val metricsComponentTypeTag: Map[String, String] = Map("component" -> componentType)
  final lazy val metricsComponentNameTag: Map[String, String] = Map("name" -> getName)
  final lazy val metricsComponentTags: Map[String, String] = metricsComponentNameTag ++ metricsComponentTypeTag

  final lazy val metricsComponentStartTimestampGauge: Metrics.Gauge =
    Metrics.newGauge("start_timestamp", Metrics.emptyMeasurementUnit, metricsComponentTags)
}

/*
 * Copyright (C) Hao Feng
 */

package atiesh.source

// internal
import atiesh.component.Component
import atiesh.metrics.{ Metrics, ComponentMetrics }

/**
 * Atiesh directory watch source semantics builtin metrics.
 */
trait DirectoryWatchSourceMetrics { this: SourceMetrics =>
  final lazy val metricsDirectoryWatchSourceOpenFilesCounter: Metrics.Counter =
    Metrics.newCounter("total_open_files", Metrics.emptyMeasurementUnit, metricsComponentTypeTag)
  final lazy val metricsDirectoryWatchSourceComponentOpenFilesCounter: Metrics.Counter =
    Metrics.newCounter("total_open_files", Metrics.emptyMeasurementUnit, metricsComponentTags)

  final lazy val metricsDirectoryWatchSourceDoneFilesCounter: Metrics.Counter =
    Metrics.newCounter("total_done_files", Metrics.emptyMeasurementUnit, metricsComponentTypeTag)
  final lazy val metricsDirectoryWatchSourceComponentDoneFilesCounter: Metrics.Counter =
    Metrics.newCounter("total_done_files", Metrics.emptyMeasurementUnit, metricsComponentTags)

  final lazy val metricsDirectoryWatchSourceInQueueItemsGauge: Metrics.Gauge =
    Metrics.newGauge("files_in_queue", Metrics.emptyMeasurementUnit, metricsComponentTypeTag)
  final lazy val metricsDirectoryWatchSourceComponentInQueueItemsGauge: Metrics.Gauge =
    Metrics.newGauge("files_in_queue", Metrics.emptyMeasurementUnit, metricsComponentTags)
}

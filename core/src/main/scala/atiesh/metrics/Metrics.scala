/*
 * Copyright (C) Hao Feng
 */

package atiesh.metrics

// kamon
import kamon.Kamon
import kamon.metric._
import kamon.system.SystemMetrics
import kamon.prometheus.PrometheusReporter
// internal
import atiesh.utils.Configuration

object Metrics {
  val reporter = new PrometheusReporter()
  val emptyTags = Map[String, String]()

  def initialize(cfg: Configuration): Unit = {
    Kamon.reconfigure(cfg.unwrapped)
    Kamon.addReporter(reporter)
    SystemMetrics.startCollecting()
  }

  def newCounter(name: String): Counter = newCounter(name, emptyTags)
  def newCounter(name: String, tags: Map[String, String]): Counter = Kamon.counter(s"Counter.${name}").refine(tags)

  def newGauge(name: String): Gauge = newGauge(name, emptyTags)
  def newGauge(name: String, tags: Map[String, String]): Gauge = Kamon.gauge(s"Gauge.${name}").refine(tags)

  def newHistogram(name: String): Histogram = newHistogram(name, emptyTags)
  def newHistogram(name: String, tags: Map[String, String]): Histogram = Kamon.histogram(s"Histogram.${name}").refine(tags)

  def newRangeSampler(name: String): RangeSampler = newRangeSampler(name, emptyTags)
  def newRangeSampler(name: String, tags: Map[String, String]): RangeSampler = Kamon.rangeSampler(s"Sampler.${name}").refine(tags)

  def newTimer(name: String): Timer = newTimer(name, emptyTags)
  def newTimer(name: String, tags: Map[String, String]): Timer = Kamon.timer(s"Timer.${name}").refine(tags)
}

/*
 * Copyright (C) Hao Feng
 */

package atiesh.metrics

// kamon
import kamon.Kamon
import kamon.MetricReporter
import kamon.metric._
import kamon.system.SystemMetrics
// scala
import scala.util.{ Try, Success, Failure }
// internal
import atiesh.utils.{ Configuration, ClassLoader, Logging }

object Metrics extends Logging {
  object ComponentOpts {
    val OPT_KAMON_REPORTERS = "kamon-reporters"
    val DEF_KAMON_REPORTERS = List[String]()
  }
  def getComponentName: String = "metrics"

  def initializeKamon(cfg: Configuration): Unit = {
    Kamon.reconfigure(cfg.unwrapped)
    SystemMetrics.startCollecting()
  }

  def initializeMetrics(mcfg: Configuration): Configuration => Try[Unit] = (cfg: Configuration) => {
    val reporters = mcfg.getStringList(ComponentOpts.OPT_KAMON_REPORTERS, ComponentOpts.DEF_KAMON_REPORTERS)
      .foldLeft(List[Try[MetricReporter]]())((reporters, className) => {
        logger.debug("loading kamon reporter class {}", className)
        val reporter = Try { ClassLoader.loadClassInstanceByName[MetricReporter](className) }
        logger.info("loaded kamon report class {}", className)
        reporter :: reporters
      })
      .foldLeft(Try(List[MetricReporter]()))((reporters, reporter) => {
        reporters.flatMap(rr => {
          reporter match {
            case Success(r) => Kamon.addReporter(r); Success(r :: rr)
            case Failure(exc) => Failure(exc)
          }
        })
      })

    /* initialize kamon only after the reporters loaded successful */
    reporters match {
      case Success(_) => Success(initializeKamon(cfg))
      case Failure(exc) => Failure(exc)
    }
  }

  val emptyMeasurementUnit = MeasurementUnit.none

  def newCounter(name: String): CounterMetric = newCounter(name, emptyMeasurementUnit)
  def newCounter(name: String, unit: MeasurementUnit): CounterMetric = Kamon.counter(s"Counter.${name}", unit)
  def newCounter(name: String, unit: MeasurementUnit, tags: Map[String, String]): Counter = newCounter(name, unit).refine(tags)

  def newGauge(name: String): GaugeMetric = newGauge(name, emptyMeasurementUnit)
  def newGauge(name: String, unit: MeasurementUnit): GaugeMetric = Kamon.gauge(s"Gauge.${name}", unit)
  def newGauge(name: String, unit: MeasurementUnit, tags: Map[String, String]): Gauge = newGauge(name, unit).refine(tags)

  def newHistogram(name: String): HistogramMetric = newHistogram(name, emptyMeasurementUnit)
  def newHistogram(name: String, unit: MeasurementUnit): HistogramMetric = Kamon.histogram(s"Histogram.${name}", unit)
  def newHistogram(name: String, unit: MeasurementUnit, tags: Map[String, String]): Histogram = newHistogram(name, unit).refine(tags)

  def newRangeSampler(name: String): RangeSamplerMetric = newRangeSampler(name, emptyMeasurementUnit)
  def newRangeSampler(name: String, unit: MeasurementUnit): RangeSamplerMetric = Kamon.rangeSampler(s"Sampler.${name}", unit)
  def newRangeSampler(name: String, unit: MeasurementUnit, tags: Map[String, String]): RangeSampler = newRangeSampler(name, unit).refine(tags)

  def newTimer(name: String): TimerMetric = Kamon.timer(s"Timer.${name}")
  def newTimer(name: String, tags: Map[String, String]): Timer = newTimer(name).refine(tags)
}

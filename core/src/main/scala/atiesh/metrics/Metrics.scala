/*
 * Copyright (C) Hao Feng
 */

package atiesh.metrics

// kamon
import kamon.Kamon
import kamon.tag.TagSet
import kamon.metric.{ Counter => MCounter, Gauge => MGauge,
                      Histogram => MHistogram, Timer => MTimer,
                      RangeSampler => MRangeSampler, _ }
// scala
import scala.util.{ Success, Failure }
import scala.concurrent.Await
import scala.concurrent.duration._
// internal
import atiesh.component.Component
import atiesh.utils.{ Logging, Configuration, ConfigParseException }

trait MetricsType {
  final val componentType: String = "kamon"
}

object Metrics extends Component with MetricsType with Logging {
  final def getName = componentType
  final def getConfiguration = metricsConfiguration

  final private[this] var metricsConfiguration: Configuration = _
  final def initializeMetrics(cfg: Configuration): Unit = {
    metricsConfiguration = cfg.getSection(componentType) match {
      case Some(c) => c
      case _ => throw new ConfigParseException(
                  "bad configurations, missing <${componentType}> section")
    }
    Kamon.init(cfg.unwrapped)
  }

  final def shutdownMetrics(): Unit =
    Await.ready(Kamon.stopModules(), Duration.Inf).value.get match {
    case Success(_) =>
      logger.info("atiesh kamon metrics modules stopped")
    case Failure(exc) =>
      logger.error("got unexpected exception during kamon shutting down, " +
                   "atiesh server force terminated", exc)
  }

  final val emptyMeasurementUnit = MeasurementUnit.none

  /* counter */
  type Counter = MCounter
  type BaseCounter = Metric.Counter
  final def newCounter(name: String): BaseCounter =
    newCounter(name, emptyMeasurementUnit)
  final def newCounter(name: String, unit: MeasurementUnit): BaseCounter =
    Kamon.counter(s"counter_${name}", unit)
  final def newCounter(name: String,
                       unit: MeasurementUnit,
                       tags: Map[String, String]): Counter =
    newCounter(name, unit).withTags(TagSet.from(tags))

  final def newCounter(root: BaseCounter,
                       tags: Map[String, String]): Counter =
    root.withTags(TagSet.from(tags))
  final def removeCounter(root: BaseCounter,
                          tags: Map[String, String]): Boolean =
    root.remove(TagSet.from(tags))

  /* gauge */
  type Gauge = MGauge
  type BaseGauge = Metric.Gauge
  final def newGauge(name: String): BaseGauge =
    newGauge(name, emptyMeasurementUnit)
  final def newGauge(name: String, unit: MeasurementUnit): BaseGauge =
    Kamon.gauge(s"gauge_${name}", unit)
  final def newGauge(name: String,
               unit: MeasurementUnit,
               tags: Map[String, String]): Gauge =
    newGauge(name, unit).withTags(TagSet.from(tags))

  final def newGauge(root: BaseGauge,
                     tags: Map[String, String]): Gauge =
    root.withTags(TagSet.from(tags))
  final def removeGauge(root: BaseGauge,
                        tags: Map[String, String]): Boolean =
    root.remove(TagSet.from(tags))

  /* histogram */
  type Histogram = MHistogram
  type BaseHistogram = Metric.Histogram
  final def newHistogram(name: String): BaseHistogram =
    newHistogram(name, emptyMeasurementUnit)
  final def newHistogram(name: String, unit: MeasurementUnit): BaseHistogram =
    Kamon.histogram(s"histogram_${name}", unit)
  final def newHistogram(name: String,
                         unit: MeasurementUnit,
                         tags: Map[String, String]): Histogram =
    newHistogram(name, unit).withTags(TagSet.from(tags))

  final def newHistogram(root: BaseHistogram,
                         tags: Map[String, String]): Histogram =
    root.withTags(TagSet.from(tags))
  final def removeHistogram(root: BaseHistogram,
                            tags: Map[String, String]): Boolean =
    root.remove(TagSet.from(tags))

  /* rangesampler */
  type RangeSampler = MRangeSampler
  type BaseRangeSampler = Metric.RangeSampler
  final def newRangeSampler(name: String): BaseRangeSampler =
    newRangeSampler(name, emptyMeasurementUnit)
  final def newRangeSampler(name: String,
                            unit: MeasurementUnit): BaseRangeSampler =
    Kamon.rangeSampler(s"sampler_${name}", unit)
  final def newRangeSampler(name: String,
                            unit: MeasurementUnit,
                            tags: Map[String, String]): RangeSampler =
    newRangeSampler(name, unit).withTags(TagSet.from(tags))

  final def newRangeSampler(root: BaseRangeSampler,
                            tags: Map[String, String]): RangeSampler =
    root.withTags(TagSet.from(tags))
  final def removeRangeSampler(root: BaseRangeSampler,
                               tags: Map[String, String]): Boolean =
    root.remove(TagSet.from(tags))

  /* timer */
  type Timer = MTimer
  type BaseTimer = Metric.Timer
  final def newTimer(name: String): BaseTimer = Kamon.timer(s"timer_${name}")
  final def newTimer(name: String, tags: Map[String, String]): Timer =
    newTimer(name).withTags(TagSet.from(tags))

  final def newTimer(root: BaseTimer, tags: Map[String, String]): Timer =
    root.withTags(TagSet.from(tags))
  final def removeTimer(root: BaseTimer, tags: Map[String, String]): Boolean =
    root.remove(TagSet.from(tags))
}

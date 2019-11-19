/*
 * Copyright (C) Hao Feng
 */

package atiesh.source

// java
import java.util.function.{ Function => JFunc,
                            BiFunction => JBiFunc }
import java.util.concurrent.{ ConcurrentHashMap => JCHashMap }
// internal
import atiesh.component.Component
import atiesh.metrics.{ Metrics, ComponentMetrics }

object KafkaSourceMetrics {
  // kafka source commit counter tags
  final val metricsKafkaSourceCommitSuccessTag = ("type" -> "commit_succeeded")
  final val metricsKafkaSourceCommitFailureTag = ("type" -> "commit_failed")
}

/**
 * Atiesh kafka source semantics builtin metrics.
 */
trait KafkaSourceMetrics { this: SourceMetrics =>
  import atiesh.metrics.KafkaMetrics._
  import KafkaSourceMetrics._

  // kafka commit root counter
  final lazy val metricsKafkaSourceCommitCounter: Metrics.BaseCounter =
    Metrics.newCounter("kafka_source_commit", Metrics.emptyMeasurementUnit)

  // commit counter for successded
  final lazy val metricsKafkaSourceCommitSuccessCounter: Metrics.Counter =
    Metrics.newCounter(metricsKafkaSourceCommitCounter,
                       metricsComponentTypeTag + metricsKafkaSourceCommitSuccessTag)
  final lazy val metricsKafkaSourceComponentCommitSuccessCounter: Metrics.Counter =
    Metrics.newCounter(metricsKafkaSourceCommitCounter,
                       metricsComponentTags + metricsKafkaSourceCommitSuccessTag)

  // commit counter for failed
  final lazy val metricsKafkaSourceCommitFailedCounter: Metrics.Counter =
    Metrics.newCounter(metricsKafkaSourceCommitCounter,
                       metricsComponentTypeTag + metricsKafkaSourceCommitFailureTag)
  final lazy val metricsKafkaSourceComponentCommitFailedCounter: Metrics.Counter =
    Metrics.newCounter(metricsKafkaSourceCommitCounter,
                       metricsComponentTags + metricsKafkaSourceCommitFailureTag)

  // rebalances counter
  final lazy val metricsKafkaSourceRebalanceCounter: Metrics.Counter =
    Metrics.newCounter("kafka_source_rebalance", Metrics.emptyMeasurementUnit, metricsComponentTypeTag)
  final lazy val metricsKafkaSourceComponentRebalanceCounter: Metrics.Counter =
    Metrics.newCounter("kafka_source_rebalance", Metrics.emptyMeasurementUnit, metricsComponentTags)

  // kafka offset root gauge
  final lazy val metricsKafkaSourceOffsetGauge: Metrics.BaseGauge =
    Metrics.newGauge("kafka_source_offset", Metrics.emptyMeasurementUnit)

  // thread-safe mapper based on the root gauge above
  final val metricsKafkaSourceOffsetGaugeMapper: JCHashMap[String, Metrics.Gauge] =
    new JCHashMap[String, Metrics.Gauge]()
  final val metricsKafkaSourceOffsetGaugeMapperInjector: JFunc[String, Metrics.Gauge] =
    new JFunc[String, Metrics.Gauge] {
      override def apply(key: String): Metrics.Gauge = {
        val (topic, partition) = metricsKafkaTopicPartitionFromKey(key)
        Metrics.newGauge(metricsKafkaSourceOffsetGauge,
                         metricsKafkaTopicPartitionTag(topic, partition) ++ metricsComponentTypeTag)
      }
    }
  final val metricsKafkaSourceOffsetGaugeMapperSweeper: JBiFunc[String, Metrics.Gauge, Metrics.Gauge] =
    new JBiFunc[String, Metrics.Gauge, Metrics.Gauge] {
      override def apply(key: String, value: Metrics.Gauge): Metrics.Gauge = {
        val (topic, partition) = metricsKafkaTopicPartitionFromKey(key)
        Metrics.removeGauge(metricsKafkaSourceOffsetGauge,
                            metricsKafkaTopicPartitionTag(topic, partition) ++ metricsComponentTypeTag)
        null
      }
    }
  final def metricsKafkaSourceRemoveTopicPartitionOffsetGauge(topic: String,
                                                              partition: Int): Metrics.Gauge =
    metricsKafkaSourceOffsetGaugeMapper.computeIfPresent(metricsKafkaTopicPartitionKey(topic, partition),
                                                         metricsKafkaSourceOffsetGaugeMapperSweeper)

  // offset gauge for topic-patition
  final def metricsKafkaSourceTopicPartitionOffsetGauge(topic: String,
                                                        partition: Int): Metrics.Gauge =
    metricsKafkaSourceOffsetGaugeMapper.computeIfAbsent(metricsKafkaTopicPartitionKey(topic, partition),
                                                        metricsKafkaSourceOffsetGaugeMapperInjector)
}

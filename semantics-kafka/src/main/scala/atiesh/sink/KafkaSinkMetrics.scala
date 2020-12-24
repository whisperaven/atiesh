/*
 * Copyright (C) Hao Feng
 */

package atiesh.sink

// java
import java.util.function.{ Function => JFunc }
import java.util.concurrent.{ ConcurrentHashMap => JCHashMap }
// internal 
import atiesh.component.Component
import atiesh.metrics.{ Metrics, ComponentMetrics }

object KafkaSinkMetrics {
  // kafka sink publish counter tags
  final val metricsKafkaSinkEventPublishSuccessTag = ("type" -> "publish_succeeded")
  final val metricsKafkaSinkEventPublishFailureTag = ("type" -> "publish_failed")
}

/**
 * Atiesh kafka sink semantics builtin metrics.
 */
trait KafkaSinkMetrics { this: SinkMetrics =>
  import atiesh.metrics.KafkaMetrics._
  import KafkaSinkMetrics._

  // event publish root counter
  final lazy val metricsKafkaSinkEventPublishCounter: Metrics.BaseCounter =
    Metrics.newCounter("sink_kafka_publish", Metrics.emptyMeasurementUnit)

  // thread-safe mapper based on the root counter above
  final val metricsKafkaSinkEventPublishCounterMapper: JCHashMap[String, Metrics.Counter] =
    new JCHashMap[String, Metrics.Counter]()

  // publish counter for succeeded
  final def metricsKafkaSinkTopicPartitionEventPublishSuccessCounter(topic: String,
                                                                     partition: Int): Metrics.Counter =
    metricsKafkaSinkEventPublishCounterMapper.computeIfAbsent(metricsKafkaTopicPartitionKey(topic, partition),
                                                              new JFunc[String, Metrics.Counter] {
      override def apply(key: String): Metrics.Counter = {
        val (topic, partition) = metricsKafkaTopicPartitionFromKey(key)
        Metrics.newCounter(metricsKafkaSinkEventPublishCounter,
                           metricsKafkaTopicPartitionTag(topic, partition) ++
                           metricsComponentTags + metricsKafkaSinkEventPublishSuccessTag)
      }
    })
  final lazy val metricsKafkaSinkEventPublishSuccessCounter: Metrics.Counter =
    Metrics.newCounter(metricsKafkaSinkEventPublishCounter,
                       metricsComponentTypeTag + metricsKafkaSinkEventPublishSuccessTag)
  final lazy val metricsKafkaSinkComponentEventPublishSuccessCounter: Metrics.Counter =
    Metrics.newCounter(metricsKafkaSinkEventPublishCounter,
                       metricsComponentTags + metricsKafkaSinkEventPublishSuccessTag)

  // publish counter for failed
  final def metricsKafkaSinkTopicPartitionEventPublishFailedCounter(topic: String,
                                                                    partition: Int): Metrics.Counter =
    metricsKafkaSinkEventPublishCounterMapper.computeIfAbsent(metricsKafkaTopicPartitionKey(topic, partition),
                                                              new JFunc[String, Metrics.Counter] {
      override def apply(key: String): Metrics.Counter = {
        val (topic, partition) = metricsKafkaTopicPartitionFromKey(key)
        Metrics.newCounter(metricsKafkaSinkEventPublishCounter,
                           metricsKafkaTopicPartitionTag(topic, partition) ++
                           metricsComponentTags + metricsKafkaSinkEventPublishFailureTag)
      }
    })
  final lazy val metricsKafkaSinkEventPublishFailedCounter: Metrics.Counter =
    Metrics.newCounter(metricsKafkaSinkEventPublishCounter,
                       metricsComponentTypeTag + metricsKafkaSinkEventPublishFailureTag)
  final lazy val metricsKafkaSinkComponentEventPublishFailedCounter: Metrics.Counter =
    Metrics.newCounter(metricsKafkaSinkEventPublishCounter,
                       metricsComponentTags + metricsKafkaSinkEventPublishFailureTag)
}

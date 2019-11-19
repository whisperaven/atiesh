/*
 * Copyright (C) Hao Feng
 */

package atiesh.metrics

object KafkaMetrics {
  val metricsKafkaTopicPartitionKeySeparator = "@"

  def metricsKafkaTopicPartitionKey(topic: String, partition: Int): String =
    s"${partition}${metricsKafkaTopicPartitionKeySeparator}${topic}"

  def metricsKafkaTopicPartitionFromKey(key: String): (String, String) =
    key.split(metricsKafkaTopicPartitionKeySeparator) match {
      case Array(topic, partition) => (topic, partition)
      case _ => ("", "0") /* should never happen */
    }

  def metricsKafkaTopicPartitionTag(topic: String,
                                    partition: String): Map[String, String] =
    Map[String, String]("topic" -> topic, "partition" -> partition)
}

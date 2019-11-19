/*
 * Copyright (C) Hao Feng
 */

package atiesh.sink

// java
import java.util.UUID.randomUUID
// internal 
import atiesh.utils.{ Configuration, Logging }

object KafkaSink {
  object KafkaSinkOpts {
    val OPT_TOPIC        = "topic"
    val OPT_TOPIC_HEADER = "topic-header"
  }

  // default impl, use uuid as message key, and no partition and timestamp
  import KafkaSinkSemantics.MetadataParser
  val metadataParser: MetadataParser =
    _ => { (Some(randomUUID().toString), None, None) }
}

/**
 * Default Kafak sink, implement on top of the LimitAck Semantics.
 */
class KafkaSink(name: String, dispatcher: String, cfg: Configuration)
  extends KafkaLimitAckSink(name, dispatcher, cfg)
  with Logging

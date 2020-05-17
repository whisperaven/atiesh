/*
 * Copyright (C) Hao Feng
 */

package atiesh.sink

// java
import java.util.UUID.randomUUID
// scala
import scala.util.{ Success, Failure }
// internal 
import atiesh.interceptor.Interceptor
import atiesh.event.Event
import atiesh.utils.{ Configuration, Logging }

object KafkaSink {
  object KafkaSinkOpts {
    val OPT_KAFKA_SINK_TOPICS = "topics"
  }
}

class KafkaSink(name: String, dispatcher: String, cfg: Configuration)
  extends AtieshSink(name, dispatcher, cfg)
  with KafkaSinkSemantics
  with Logging {
  import KafkaSink.{ KafkaSinkOpts => Opts }
  import KafkaSinkSemantics.MetadataParser

  val topics = cfg.getStringList(Opts.OPT_KAFKA_SINK_TOPICS)

  // default impl, use uuid as message key, and no partition and timestamp
  val parser: MetadataParser =
    _ => { (Some(randomUUID().toString), None, None) }

  def accept(event: Event): Boolean = true

  def process(event: Event): Unit = {
    logger.debug("kafka sink <{}> drain event <{}>", getName, event.getBody)

    topics.foreach(topic => {
      kafkaSend(event, topic, parser).onComplete({
        case Success(metadata) =>
          logger.debug(
            "produce message <{}> to kafka topic <{}> successed, with " +
            "partition <{}> and timestamp <{}>, kafka offset was <{}>",
            event.getBody, topic,
            metadata.partition, metadata.timestamp, metadata.offset)
        case Failure(exc) =>
          logger.debug(s"produce message <${event.getBody}> to kafka " +
                       s"topic <${topic}> failed", exc)
      })(getKafkaExecutionContext)
    })
  }

  def shutdown(): Unit = {
    logger.info("shutting down sink <{}>", getName)
  }

  def startup(): Unit = {
    logger.info("starting sink <{}>", getName)
  }
}

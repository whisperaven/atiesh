/*
 * Copyright (C) Hao Feng
 */

package atiesh.sink

// java
import java.util.UUID.randomUUID
// scala
import scala.util.{ Success, Failure }
import scala.concurrent.ExecutionContext
// internal
import atiesh.interceptor.Interceptor
import atiesh.event.Event
import atiesh.utils.{ Configuration, Logging }

class KafkaSink(name: String, dispatcher: String, cfg: Configuration)
  extends AtieshSink(name, dispatcher, cfg)
  with KafkaSinkSemantics
  with Logging {
  object KafkaSinkOpts {
    val OPT_KAFKA_SINK_TOPICS = "topics"
  }

  implicit var kec: ExecutionContext = _
  val topics = cfg.getStringList(KafkaSinkOpts.OPT_KAFKA_SINK_TOPICS)

  // use uuid as message key, and no partition and timestamp
  val parser: Event => (Option[String], Option[Int], Option[Long]) = 
    e => { (Some(randomUUID().toString), None, None) }

  def accept(event: Event): Boolean = true

  def process(event: Event): Unit = {
    logger.debug("kafka sink <{}> drain event <{}>", getName, event.getBody)
    topics.foreach(topic => {
      send(event, topic)(parser).onComplete({
        case Success(metadata) =>
          logger.debug(
            "produce message <{}> to kafka topic <{}> successed, with partition " +
            "<{}> and timestamp <{}>, kafka offset was <{}>",
            event.getBody, topic,
            metadata.partition, metadata.timestamp, metadata.offset)
        case Failure(exc) =>
          logger.debug(s"produce message <${event.getBody}> to kafka topic <${topic}> failed", exc)
      })
    })
  }

  def shutdown(): Unit = { 
    logger.info("shutting down sink <{}>", getName)
  }

  def startup(): Unit = { 
    kec = getExecutionContext
    logger.info("starting sink <{}>", getName)
  }
}

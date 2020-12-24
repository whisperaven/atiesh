/*
 * Copyright (C) Hao Feng
 */

package atiesh.sink

// scala
import scala.util.{ Try, Success, Failure }
// kafka
import org.apache.kafka.clients.producer.RecordMetadata
// internal 
import atiesh.event.Event
import atiesh.utils.{ Configuration, Logging }

object KafkaSynchronousAckSink {
  object KafkaSynchronousAckSinkOpts {
    val OPT_EVENT_CREATOR_HEADERNAME = "event-creator-headername"
  }
}

/**
 * Kafak sink implement on top of the SynchronousAck Semantics.
 */
class KafkaSynchronousAckSink(name: String,
                              dispatcher: String,
                              cfg: Configuration)
  extends AtieshSink(name, dispatcher, cfg)
  with KafkaSynchronousAckSinkSemantics
  with Logging {
  import KafkaSink.{ KafkaSinkOpts => Opts, _ }
  import KafkaSinkSemantics.MetadataParser
  import KafkaSynchronousAckSink.{ KafkaSynchronousAckSinkOpts => KSOpts }

  val cfgTopic = cfg.getString(Opts.OPT_TOPIC)
  val cfgTopicHeader = cfg.getStringOption(Opts.OPT_TOPIC_HEADER)

  val cfgEventCreatorHeadername =
    cfg.getString(KSOpts.OPT_EVENT_CREATOR_HEADERNAME)

  // default impl, get headername via configuration system
  def kafkaEventCreatorHeadername: String = cfgEventCreatorHeadername

  // default impl, use uuid as message key, and no partition and timestamp
  val parser: MetadataParser = metadataParser

  // default impl, accept everything
  def accept(event: Event): Boolean = true

  // default impl, log error when produce failed
  def kafkaResponseHandler(
    event: Event, topic: String): PartialFunction[Try[RecordMetadata], Unit] = {
    case Success(meta) =>
      logger.debug("sink <{}> produce message <{}> to kafka " +
                   "topic <{} (partition: {} / offset: {}> successful",
                   getName, event.getBody, topic,
                   meta.partition(), meta.offset())
    case Failure(exc) =>
      logger.error(s"sink <${getName}> produce message " +
                   s"<${event.getBody}> to kafka topic " +
                   s"<${topic}> failed", exc)
  }

  def kafkaTopics(event: Event): List[String] =
    List(cfgTopicHeader.flatMap(th => event.getHeaders.get(th))
                       .getOrElse(cfgTopic))

  def kafkaPublish(event: Event): Unit =
    kafkaTopics(event).foreach(topic => {
      logger.debug("sink <{}> produce message <{}> to topic <{}>",
                   getName, event.getBody, topic)
      kafkaProduce(event, topic, parser)
    })

  def process(event: Event): Unit = {
    logger.debug("kafka sink <{}> drain event <{}>", getName, event.getBody)
    kafkaPublish(event)
  }

  def startup(): Unit = logger.info("starting sink <{}>", getName)
  def shutdown(): Unit = logger.info("shutting down sink <{}>", getName)
}

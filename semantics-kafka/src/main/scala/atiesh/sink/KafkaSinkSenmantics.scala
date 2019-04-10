/*
 * Copyright (C) Hao Feng
 */

package atiesh.sink

// java
import java.util.Properties
// scala
import scala.concurrent.{ Promise, Future }
import scala.collection.JavaConverters._
// kafka
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord, RecordMetadata, Callback }
// internal
import atiesh.event.Event
import atiesh.statement.{ Ready, Closed }
import atiesh.utils.{ Configuration, Logging }

object KafkaSinkSemantics {
  object KafkaSinkSemanticsOpts {
    val OPT_PRODUCER_PROPERTIES_SECTION = "kafka-properties"
  }
}

trait KafkaSinkSemantics extends SinkSemantics with Logging { this: Sink =>
  import KafkaSinkSemantics._

  private var producer: KafkaProducer[String, String] = _
  def createProducer(name: String, pcf: Option[Configuration]): KafkaProducer[String, String] = {
    val props = new Properties()
    pcf match {
      case Some(c) =>
        for ((k, v) <- c.entrySet) {
          props.put(k, v)
        }
      case None =>
        throw new SinkInitializeException("can not initialize kafka sink semantics, missing producer properties")
    }

    val producer = new KafkaProducer[String, String](props)
    logger.debug("sink <{}> producer created", name)

    producer
  }
  
  def createProducerCB(p: Promise[RecordMetadata]): Callback = {
    new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception == null) {
          p.success(metadata)
        } else {
          p.failure(exception)
        }
      }
    }
  }

  def send(event: Event, topic: String)
          (parser: Event => (Option[String], Option[Int], Option[Long])): Future[RecordMetadata] = {
    val record = parser(event) match {
      case (None, None, None) =>
        Some(new ProducerRecord[String, String](topic, event.getBody))
      case (Some(key), None, None) =>
        Some(new ProducerRecord[String, String](topic, key, event.getBody))
      case (Some(key), Some(partation), None) =>
        Some(new ProducerRecord[String, String](topic, partation, key, event.getBody))
      case (Some(key), Some(partation), Some(timestamp)) =>
        Some(new ProducerRecord[String, String](topic, partation, timestamp, key, event.getBody))
      case _ =>
        None
    }

    record match {
      case Some(r) =>
        val promise = Promise[RecordMetadata]()
        producer.send(r, createProducerCB(promise))
        promise.future
      case None =>
        Future.failed[RecordMetadata](new SinkInvalidEventException(
          "bad event parser response, cannot create record instance, the parser should return a tuple contains" +
          "<None, None, None> or <key, None, None> or <key, partation, None> or <key, partation, timestamp>"))
    }
  }

  override def start(ready: Promise[Ready]): Unit = {
    val cfg = getConfiguration
    val pcf = cfg.getSection(KafkaSinkSemanticsOpts.OPT_PRODUCER_PROPERTIES_SECTION)

    producer = createProducer(getName, pcf)

    super.start(ready)
  }

  override def stop(closed: Promise[Closed]): Unit = {
    logger.debug("sink <{}> producer flush all records", getName)
    producer.flush()
    logger.debug("sink <{}> closing producer instance", getName)
    producer.close()

    super.stop(closed)
  }
}

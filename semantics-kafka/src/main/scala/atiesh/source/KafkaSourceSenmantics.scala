/*
 * Copyright (C) Hao Feng
 */

package atiesh.source

// java
import java.util.Properties
// scala
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.collection.JavaConverters._
// akka
import akka.actor.ActorSystem
// kafka
import org.apache.kafka.clients.consumer.{ KafkaConsumer, ConsumerRecords }
// internal
import atiesh.event.{ Event, Empty, SimpleEvent }
import atiesh.statement.Closed
import atiesh.utils.{ Configuration, Logging }
import atiesh.metrics.MetricsGroup._

object KafkaSourceSemantics {
  object KafkaSourceSemanticsHeadersName {
    val TOPIC = "kafkaTopic"
    val PARTITION = "kafkaPartition"
  }
}

trait KafkaSourceSemantics extends SourceSemantics with Logging { this: Source =>
  import KafkaSourceSemantics._

  object KafkaSourceSemanticsOpts {
    val OPT_CONSUMER_SETTINGS_SECTION = "client-settings"

    val OPT_CONSUMER_TOPICS = "topics"
    val DEF_CONSUMER_TOPICS = List[String]()

    val OPT_CONSUMER_POLL_TIMEOUT = "poll-timeout"
    val DEF_CONSUMER_POLL_TIMEOUT = FiniteDuration(1000, MILLISECONDS)
  }

  private var pollTimeout: FiniteDuration = _
  private var consumer: KafkaConsumer[String, String] = _
  def createConsumer(name: String, ccf: Option[Configuration], topics: List[String]): KafkaConsumer[String, String] = {
    val props = new Properties()
    ccf match {
      case Some(c) =>
        for ((k, v) <- c.entrySet) {
          props.put(k, v)
        }
      case None =>
        throw new SourceInitializeException("can not initialize kafka source semantics, missing consumer settings")
    }

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(topics.asJava)
    logger.debug("source <{}> consumer created and subscribe topic(s): {}", name, topics)

    consumer
  }
  def getConsumer(): KafkaConsumer[String, String] = consumer

  override def bootstrap()(implicit system: ActorSystem): Source = {
    super.bootstrap()

    val cfg = getConfiguration
    val topics = cfg.getStringList(
      KafkaSourceSemanticsOpts.OPT_CONSUMER_TOPICS,
      KafkaSourceSemanticsOpts.DEF_CONSUMER_TOPICS)

    logger.debug("source <{}> initialize kafka consumer instances", getName)
    val ccf = cfg.getSection(KafkaSourceSemanticsOpts.OPT_CONSUMER_SETTINGS_SECTION)

    pollTimeout = cfg.getDuration(
      KafkaSourceSemanticsOpts.OPT_CONSUMER_POLL_TIMEOUT,
      KafkaSourceSemanticsOpts.DEF_CONSUMER_POLL_TIMEOUT)
    consumer = createConsumer(getName, ccf, topics)

    this
  }

  def mainCycle(): Unit = {
    val records =
      try {
        logger.debug("source <{}> cycle start, polling from kafka", getName)
        consumer.poll(pollTimeout.toMillis)
      } catch {
        case exc: Throwable =>
          logger.error("got unexpected exception while polling from kafka broker", exc)
          ConsumerRecords.empty(): ConsumerRecords[String, String]
      }

    if (!records.isEmpty()) {
      for (r <- records.asScala; v = r.value) {
        if (v != null) {
          val event = SimpleEvent(v, Map[String, String](
            KafkaSourceSemanticsHeadersName.TOPIC -> r.topic(),
            KafkaSourceSemanticsHeadersName.PARTITION -> r.partition().toString))
            SourceMetrics.eventAcceptedCount.increment()

          intercept(event) match {
            case Empty =>
              logger.debug("event <{}> discarded by interceptor", event.getBody)

            case intercepted: Event =>
              sink(intercepted)
          }
        } else {
          logger.warn("got empty record from kafka, ignore and process next one")
        }
      }
      commit()
    }
  }

  override def stop(closed: Promise[Closed]): Unit = {
    logger.debug("source <{}> consumer committing offset", getName)
    consumer.commitSync()
    logger.debug("source <{}> closing consumer instance", getName)
    consumer.close()

    super.stop(closed)
  }
}

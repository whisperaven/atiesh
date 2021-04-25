/*
 * Copyright (C) Hao Feng
 */

package atiesh.sink

// java
import java.util.Properties
// scala
import scala.util.{ Try, Success, Failure }
import scala.concurrent.{ ExecutionContext, Promise, Future }
import scala.collection.JavaConverters._
// akka
import akka.actor.ActorSystem
// kafka
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord,
                                           RecordMetadata, Callback }
// internal
import atiesh.event.Event
import atiesh.statement.{ Ready, Closed }
import atiesh.utils.{ Configuration, Logging }

object KafkaSinkSemantics extends Logging {
  object KafkaSinkSemanticsOpts {
    val OPT_KAFKA_PRODUCER_PROPERTIES_SECTION = "kafka-properties"

    val OPT_AKKA_DISPATCHER = "kafka-akka-dispatcher"
    val DEF_AKKA_DISPATCHER = "akka.actor.default-dispatcher"
  }

  // parse each record into (uuid, partition, timestamp)
  type MetadataParser = Event => (Option[String], Option[Int], Option[Long])

  def createProducer(
    owner: String,
    pcf: Option[Configuration]): KafkaProducer[String, String] = {
    val props = new Properties()
    pcf match {
      case Some(c) =>
        for ((k, v) <- c.entrySet) {
          props.put(k, v)
        }
      case None =>
        throw new SinkInitializeException(
          "can not initialize kafka sink semantics, " +
          "missing producer properties")
    }

    val producer = new KafkaProducer[String, String](props)
    logger.debug("sink <{}> producer created", owner)

    producer
  }
}

trait KafkaSinkSemantics
  extends SinkSemantics
  with KafkaSinkMetrics
  with Logging { this: Sink =>
  import KafkaSinkSemantics.{ KafkaSinkSemanticsOpts => Opts, _ }

  final private[this] var kafkaDispatcher: String = _
  final private[this] var kafkaExecutionContext: ExecutionContext = _

  final def getKafkaDispatcher: String = kafkaDispatcher
  final def getKafkaExecutionContext: ExecutionContext = kafkaExecutionContext

  final private[this] var kafkaProducer: KafkaProducer[String, String] = _

  override def bootstrap()(implicit system: ActorSystem): Unit = {
    super.bootstrap()

    kafkaDispatcher = getConfiguration.getString(Opts.OPT_AKKA_DISPATCHER,
                                                 Opts.DEF_AKKA_DISPATCHER)
    kafkaExecutionContext = system.dispatchers.lookup(kafkaDispatcher)
  }

  final def createProduceCB(p: Promise[RecordMetadata]): Callback =
    new Callback {
      override def onCompletion(metadata: RecordMetadata,
                                exception: Exception): Unit = {
        if (exception == null) {
          metricsKafkaSinkTopicPartitionEventPublishSuccessCounter(
            metadata.topic, metadata.partition).increment()
          metricsKafkaSinkEventPublishSuccessCounter.increment()
          metricsKafkaSinkComponentEventPublishSuccessCounter.increment()
          p.success(metadata)
        } else {
          if (metadata != null) {
            metricsKafkaSinkTopicPartitionEventPublishFailedCounter(
              metadata.topic, metadata.partition).increment()
          }
          metricsKafkaSinkEventPublishFailedCounter.increment()
          metricsKafkaSinkComponentEventPublishFailedCounter.increment()
          p.failure(exception)
        }
      }
    }

  /**
   * According to the document of <Kafka Client>, the producer may block
   * maximum <max.block.ms> mills when the internal buffer is filled (which
   * maximum <buffer.memory> bytes) or metadata unavailable.
   *
   * The Future instance returned by this method represent the send result
   * of event which is already append to the internal buffer of the producer
   * instance. After buffer is filled, the send method will be block.
   */
  final def kafkaSend(event: Event,
                      topic: String,
                      parser: MetadataParser): Future[RecordMetadata] = {
    val record = Try {
      parser(event) match {
        case (None, None, None) =>
          Some(new ProducerRecord[String, String](topic, event.getBody))
        case (Some(key), None, None) =>
          Some(new ProducerRecord[String, String](topic, key, event.getBody))
        case (Some(key), Some(partation), None) =>
          Some(new ProducerRecord[String, String](topic, partation,
                                                  key, event.getBody))
        case (Some(key), Some(partation), Some(timestamp)) =>
          Some(new ProducerRecord[String, String](topic, partation, timestamp,
                                                  key, event.getBody))
        case _ =>
          None
      }
    }

    record match {
      case Success(Some(r)) =>
        try {
          val p = Promise[RecordMetadata]()
          kafkaProducer.send(r, createProduceCB(p))
          p.future
        } catch {
          case exc: Throwable =>
            Future.failed[RecordMetadata](exc)
        }
      case Success(_) =>
        Future.failed[RecordMetadata](new SinkInvalidEventException(
          "bad event parser response, cannot create record instance, the " +
          "parser should return a tuple which contains <None, None, None> " +
          "or <key, None, None> or <key, partation, None> " +
          "or <key, partation, timestamp>"))
      case Failure(exc) =>
        Future.failed[RecordMetadata](new SinkInvalidEventException(
          "bad event parser, got unexcepted exception during kafka event " +
          "parse, cannot create record instance", exc))
    }
  }

  final def kafkaFlush(): Unit = {
    try {
      kafkaProducer.flush()
    } catch {
      case exc: Throwable =>
        logger.error(
          s"sink <${getName}> cannot flush the kafka producer instance", exc)
    }
  }

  override def open(ready: Promise[Ready]): Unit = {
    val cfg = getConfiguration

    kafkaProducer =
      createProducer(getName,
                     cfg.getSection(Opts.OPT_KAFKA_PRODUCER_PROPERTIES_SECTION))

    super.open(ready)
  }

  override def close(closed: Promise[Closed]): Unit = {
    logger.debug("sink <{}> producer flushing all records", getName)
    kafkaProducer.flush()

    logger.debug("sink <{}> flushed all records, closing producer instance",
                 getName)
    kafkaProducer.close()

    super.close(closed)
  }
}

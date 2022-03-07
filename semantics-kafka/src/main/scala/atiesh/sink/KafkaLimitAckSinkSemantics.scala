/*
 * Copyright (C) Hao Feng
 */

package atiesh.sink

// java
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ ConcurrentHashMap => JCHashMap }
// scala
import scala.util.{ Try, Success, Failure }
import scala.concurrent.{ Promise, Future }
import scala.collection.JavaConverters._
// kafka
import org.apache.kafka.clients.producer.RecordMetadata
// internal
import atiesh.event.Event
import atiesh.statement.{ Ready, Transaction, Closed }
import atiesh.utils.{ Configuration, Logging }

object KafkaLimitAckSinkSemantics {
  object KafkaLimitAckSinkSemanticsOpts {
    val OPT_MAX_PENDING_ACKS = "max-pending-acks"
    val DEF_MAX_PENDING_ACKS = 384

    val OPT_MUST_SEND = "must-send"
    val DEF_MUST_SEND = false
    val OPT_ENABLE_ACTIVE_FLUSH = "enable-active-flush"
    val DEF_ENABLE_ACTIVE_FLUSH = false
  }

  object KafkaLimitAckSinkSemanticsSignals {
    val SIG_RESUME_PROCESS: Int = 1
  }
}

trait KafkaLimitAckSinkSemantics
  extends KafkaSinkSemantics
  with Logging { this: Sink =>
  import KafkaLimitAckSinkSemantics.{ KafkaLimitAckSinkSemanticsOpts => Opts,
                                      KafkaLimitAckSinkSemanticsSignals => Sig }
  import KafkaSinkSemantics.MetadataParser

  /* cfgs are writen once on start inside open */
  final private[this] var cfgMaxPendingAcks: Int = _
  final private[this] var cfgMustSend: Boolean = false
  final private[this] var cfgActiveFlush: Boolean = false

  /**
   * Internal API - KafkaProduce
   *
   * Thread safe method, produce message to kafka borker, and retry
   * if <must-send> was set to <true>, also manage total pending acks
   * to prevent too many future instances of messages overflow the engine.
   */
  final private def kafkaProduce(event: Event,
                                 topic: String,
                                 parser: MetadataParser,
                                 isRetry: Boolean): Unit = {
    if (!isRetry) pendingAcks.incrementAndGet()

    Try {
      kafkaSend(event, topic, parser)
    } match {
      case Success(req) =>
        req.onComplete({
          case Success(metadata) =>
            logger.debug(
              "sink <{}> produce message <{}> to kafka topic <{}> successed," +
              " with partition <{}> and timestamp <{}>, kafka offset was <{}>",
              getName, event.getBody, topic,
              metadata.partition, metadata.timestamp, metadata.offset)

            try {
              kafkaResponseHandler(event, topic)(Try(metadata))
            } catch {
              case exc: Throwable =>
                logger.error(s"sink <${getName}> throw unexcepted exception " +
                             s"inside user define <kafkaResponseHandler>, " +
                             s"which means you may use a kafka sink " +
                             s"component with wrong implementation", exc)
            }

            logger.debug("sink <{}> got total <{}> pending acks, check " +
                         "for process resume after event send successed",
                         getName, pendingAcks.get())
            signal(Sig.SIG_RESUME_PROCESS)
          case Failure(exc) =>
            try {
              /*
               * let the user known what happened when something goes wrong,
               * but they cant interfere, it's depends on <cfgMustSend> only
               */
              kafkaResponseHandler(event,
                                   topic)(Try[RecordMetadata]({ throw exc }))
            } catch {
              case exc: Throwable =>
                logger.error(s"sink <${getName}> throw unexcepted exception " +
                             s"inside user define <kafkaResponseHandler>, " +
                             s"which means you may use a kafka sink " +
                             s"component with wrong implementation", exc)
            }

            if (cfgMustSend) {
              kafkaProduce(event, topic, parser, true)
            } else {
              logger.debug("sink <{}> got total <{}> pending acks, check " +
                           "for process resume after event send failed",
                           getName, pendingAcks.get())
              signal(Sig.SIG_RESUME_PROCESS)
            }
        })(getKafkaExecutionContext)
      case Failure(exc) =>
        logger.error(
          s"sink <${getName}> got unexpected message produce exception " +
          s"while sending event <${event.getBody}> to kafka topic " +
          s"<${topic}>, retring", exc)
        kafkaProduce(event, topic, parser, true)  /* do it again */
    }
  }

  /**
   * High-Level API - KafkaHandleResponse
   *
   * Not thread safe method, user defined callback for handle kafka response.
   */
  def kafkaResponseHandler(
    event: Event, topic: String): PartialFunction[Try[RecordMetadata], Unit]

  /**
   * High-Level API - KafkaProduce
   *
   * Thread safe method, produce message to kafka borker, and retry
   * if <must-send> was set to <true>, also manage total pending acks
   * to prevent too many future instances of messages overflow the engine.
   */
  final def kafkaProduce(event: Event,
                         topic: String,
                         parser: MetadataParser): Unit =
    kafkaProduce(event, topic, parser, false)

  /**
   * High-Level API - kafkaProducerIsClosing
   *
   * Thread safe method, Return true if the KafkaLimitAckSinkSemantics
   * receive a close statement.
   */
  final def kafkaProducerIsClosing: Boolean = !closing.isEmpty

  override def open(ready: Promise[Ready]): Unit = {
    val cfg = getConfiguration
    cfgMaxPendingAcks = cfg.getInt(Opts.OPT_MAX_PENDING_ACKS,
                                   Opts.DEF_MAX_PENDING_ACKS)
    cfgMustSend = cfg.getBoolean(Opts.OPT_MUST_SEND, Opts.DEF_MUST_SEND)
    cfgActiveFlush = cfg.getBoolean(Opts.OPT_ENABLE_ACTIVE_FLUSH,
                                    Opts.DEF_ENABLE_ACTIVE_FLUSH)

    super.open(ready)
  }

  override def process(sig: Int): Unit = {
    logger.debug("sink <{}> handle signal <{}>", getName, sig)

    sig match {
      /**
       * handle resume signal
       */
      case Sig.SIG_RESUME_PROCESS =>
        val pendings = pendingAcks.decrementAndGet()

        if (!transactions.isEmpty && pendings < cfgMaxPendingAcks) {
          transactions.iterator().asScala
            .foreach({
              case (committer, tran) =>
                logger.debug("sink <{}> acknowledge delayed commit from <{}>" +
                             "transaction(s), current <{}> pending acks",
                             getName, committer, pendings)
                super.ack(committer, tran)
            })
          transactions.clear()
        }

        if (pendings == 0) closing.map(closed => super.close(closed))

      /**
       * handle other illegal signals.
       */
      case _ =>
        if (sig < 0) {
          super.process(sig)  /* passing-through core signals */
        } else {
          logger.error("kafka sink semantics of sink <{}> got illegal " +
                       "signal num <{}> which means you may use a " +
                       "kafka sink with wrong implementation", getName, sig)
        }
    }
  }

  final private[this] val pendingAcks = new AtomicLong(0)
  final private[this] val transactions =
    JCHashMap.newKeySet[(String, Promise[Transaction])]
  override def ack(committer: String, tran: Promise[Transaction]): Unit = {
    /**
     * Flush the producer instances before handle
     * the incoming ack statement from Source component.
     */
    if (cfgActiveFlush) kafkaFlush()

    if (pendingAcks.get() < cfgMaxPendingAcks) {
      super.ack(committer, tran)
    } else {
      logger.info("sink <{}> got too many (total <{}>) pending acks " +
                  "exceed max-pending-acks, going to delay transition " +
                  "commit <{}@{}>, which will slow down the sources",
                  getName, pendingAcks.get, tran, tran.hashCode.toHexString)
      transactions.add((committer -> tran))
    }
  }

  @volatile final private[this] var closing: Option[Promise[Closed]] = None
  override def close(closed: Promise[Closed]): Unit = {
    /**
     * Flush the producer instances before handle
     * the incoming close statement from Source component.
     */
    kafkaFlush()

    if (pendingAcks.get() != 0) {
      logger.info("sink <{}> still have <{}> pending acks, " +
                  "delay close <{}@{}>", getName, pendingAcks.get(), closed,
                                         closed.hashCode.toHexString)
      closing = Some(closed)
    } else {
      super.close(closed)
    }
  }
}

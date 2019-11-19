/*
 * Copyright (C) Hao Feng
 */

package atiesh.sink

// java
import java.util.concurrent.{ ConcurrentHashMap => JCHashMap }
import java.util.function.{ BiFunction => JBiFunc }
// scala
import scala.util.{ Try, Success, Failure }
import scala.concurrent.{ Promise, Future }
import scala.collection.JavaConverters._
import scala.collection.mutable
// kafka
import org.apache.kafka.clients.producer.RecordMetadata
// internal 
import atiesh.event.Event
import atiesh.statement.{ Ready, Transaction, Closed }
import atiesh.utils.{ Configuration, Logging }

object KafkaSynchronousAckSinkSemantics {
  object KafkaSynchronousAckSinkSemanticsOpts {
    val OPT_IN_FLIGHT_REQUESTS_SIZE = "in-flight-request-size"
    val DEF_IN_FLIGHT_REQUESTS_SIZE = 128
  }

  object KafkaSynchronousAckSinkSemanticsSignals {
    val SIG_HANDLE_ACK: Int = 1
  }
}

trait KafkaSynchronousAckSinkSemantics
  extends KafkaSinkSemantics
  with Logging { this: Sink =>
  import KafkaSynchronousAckSinkSemantics.{
    KafkaSynchronousAckSinkSemanticsOpts => Opts,
    KafkaSynchronousAckSinkSemanticsSignals => Sig }
  import KafkaSinkSemantics.MetadataParser

  /* queues of requests for implement synchronous ack */
  final private[this] var inFlightRequests:
    JCHashMap[String, List[Future[RecordMetadata]]] = _

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
   * Produce message to kafka borker, also manage total pre-component acks
   * to make sure all events are processed before make an ack.
   *
   * Do not invoke this outside the sink's process method, or inside future
   * callback to prevent async ack from happening.
   */
  final def kafkaProduce(event: Event,
                         topic: String,
                         parser: MetadataParser): Unit = {
    val req = kafkaSend(event, topic, parser)
    event.getHeaders().get(kafkaEventCreatorHeadername) match {
      case Some(creator) =>
        val injector = new JBiFunc[String, List[Future[RecordMetadata]],
                                           List[Future[RecordMetadata]]] {
          override def apply(key: String,
                             value: List[Future[RecordMetadata]]):
            List[Future[RecordMetadata]] = if (value == null) List(req)
                                           else req :: value
        }
        val q = inFlightRequests.compute(creator, injector)
        logger.debug("sink <{}> got total <{}> requests from creator <{}>",
                     getName, q.length, creator)
      case None =>
        logger.warn("sink <{}> cannot fulfill synchronous ack because of " +
                    "missing header <{}>", getName, kafkaEventCreatorHeadername)
    }
    req.onComplete(kafkaResponseHandler(event, topic))(getKafkaExecutionContext)
  }

  /**
   * High-Level API - kafkaProducerIsClosing
   *
   * Thread safe method, Return true if the KafkaLimitAckSinkSemantics
   * receive a close statement.
   */
  final def kafkaProducerIsClosing: Boolean = !closing.isEmpty

  /**
   * High-Level API - kafakEventCreatorHeadername
   *
   * Thread safe method, Return header name which indicate the creator
   * of that event.
   */
  def kafkaEventCreatorHeadername: String

  override def open(ready: Promise[Ready]): Unit = {
    val cfg = getConfiguration
    inFlightRequests =
      new JCHashMap[String, List[Future[RecordMetadata]]](
        cfg.getInt(Opts.OPT_IN_FLIGHT_REQUESTS_SIZE,
                   Opts.DEF_IN_FLIGHT_REQUESTS_SIZE))

    super.open(ready)
  }

  override def process(sig: Int): Unit = {
    logger.debug("sink <{}> handle signal <{}>", getName, sig)

    sig match {
      /**
       * handle ack signal
       */
      case Sig.SIG_HANDLE_ACK =>
        pendingTrans.foldLeft(List[Promise[Transaction]]())({
          case (ts, (tran, ctx)) =>
            if (ctx._2.isCompleted) tran :: ts else ts
        }).foreach(tran => pendingTrans.remove(tran).map(ctx => {
          ctx._2.value.get match {
            case Success(_)   => super.ack(ctx._1, tran)
            case Failure(exc) => tran.failure(exc)
          }
          logger.debug("sink <{}> handled ack synchronous from source " +
                       "<{}> (<{}@{}>)",
                       getName, ctx._1, tran, tran.hashCode.toHexString)
        }))

        if (pendingTrans.size == 0) closing.map(closed => close(closed))

      /**
       * handle other illegal signals.
       */
      case _ =>
        logger.error("kafka synchronous ack sink semantics of sink <{}> got " +
                     "illegal signal num <{}> which means you may use a " +
                     "kafka sink with wrong implementation", getName, sig)
    }
  }

  final private[this] val pendingTrans =
    mutable.Map[Promise[Transaction],
                (String, Future[List[RecordMetadata]])]()
  override def ack(committer: String, tran: Promise[Transaction]): Unit = {
    /**
     * Flush the producer instances before handle
     * the incoming ack statement from Source component.
     */
    kafkaFlush()

    val reqs = inFlightRequests.remove(committer)
    if (reqs == null) {
      logger.warn("sink <{}> cannnot retrieve any requests for calculate " +
                  "and achieve synchronous ack, you may use wrong component " +
                  "which implement with an improper semantics", getName)
    } else {
      implicit val executor = getKafkaExecutionContext
      val done = Future.sequence(reqs)
      pendingTrans += (tran -> (committer -> done))

      logger.debug("sink <{}> handle acknowledging commit transaction from " +
                   "committer <{}> (<{}@{}>) synchronous",
                   getName, committer, tran, tran.hashCode.toHexString)
      done.onComplete(_ => signal(Sig.SIG_HANDLE_ACK))(getKafkaExecutionContext)
    }
  }

  @volatile final private[this] var closing: Option[Promise[Closed]] = None
  override def close(closed: Promise[Closed]): Unit = {
    /**
     * Flush the producer instances before handle
     * the incoming close statement from Source component.
     */
    kafkaFlush() 

    if (pendingTrans.size != 0) {
      logger.info("sink <{}> still have <{}> pending acks, " +
                  "delay close <{}@{}>",
                  getName, pendingTrans.size,
                  closed, closed.hashCode.toHexString)
      closing = Some(closed)
    } else {
      super.close(closed)
    }
  }
}

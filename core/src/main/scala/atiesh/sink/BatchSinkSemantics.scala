/*
 * Copyright (C) Hao Feng
 */

package atiesh.sink

// java
import java.util.concurrent.{ ConcurrentHashMap => JCHashMap }
import java.util.function.{ BiFunction => JBiFunc }
// scala
import scala.util.{ Success, Failure }
import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Promise, Await }
// akka
import akka.actor.{ Props, Actor, ActorRef,
                    ActorSystem, Scheduler, Cancellable }
import akka.event.LoggingReceive
// internal
import atiesh.event.Event
import atiesh.statement.{ Close, Closed, Batch }
import atiesh.utils.{ Configuration, Logging }

object BatchSinkSemantics {
  object BatchSinkSemanticsOpts {
    /* 0 meanes infinite size */
    val OPT_BATCH_SIZE = "batch-size"
    val DEF_BATCH_SIZE = 0
    /* 0 meanes infinite timeout */
    val OPT_BATCH_TIMEOUT = "batch-timeout"
    val DEF_BATCH_TIMEOUT = Duration(0, MILLISECONDS)
    val OPT_BATCH_AKKA_DISPATCHER = "batch-akka-dispatcher"
    val DEF_BATCH_AKKA_DISPATCHER = "akka.actor.default-dispatcher"
  }
  val DEF_BATCH_TAG = "__atiesh_default__"

  private[sink] object BatchBuffer extends Logging {
    def apply(size: Int): BatchBuffer =
      new BatchBuffer(size, None)
    def apply(size: Int, event: Event): BatchBuffer =
      new BatchBuffer(size, Some(event))
  }

  private[sink] class BatchBuffer(size: Int,
                                  initial: Option[Event]) extends Logging {
    final private[this] val buffer = initial match {
      case Some(event) =>
        mutable.ArrayBuffer[Event](event)
      case None =>
        mutable.ArrayBuffer[Event]()
    }
    final def isFull: Boolean = size > 0 && buffer.length >= size
    final def append(event: Event): Unit = buffer.append(event)
    final def asBatch(tag: String): Batch = Batch(buffer.toList, tag)
  }

  private[sink] case class Flush(tag: String, timeout: Boolean)
  private[sink] case class FlushableBatchBuffer(buf: BatchBuffer,
                                                task: Option[Cancellable]) {
    final def isFull: Boolean = buf.isFull
    final def append(event: Event): Unit = buf.append(event)
    final def asBatch(tag: String): Batch = buf.asBatch(tag)

    final def cancel: Boolean = task match {
      case Some(cancelable) => cancelable.cancel()
      case _ => true
    }
  }
}

/**
 * Atiesh semantics which represent a sink component with batch supported.
 */
trait BatchSinkSemantics
  extends SinkSemantics
  with Logging { this: Sink =>
  import BatchSinkSemantics.{ BatchSinkSemanticsOpts => Opts, _ }
  object BatchManagerActor {
    def props(): Props = Props(new BatchManagerActor())
  }
  final private class BatchManagerActor extends Actor with Logging {
    override def receive: Receive = LoggingReceive {
      case event: Event =>
        val tag = batchTag(event)
        val buf = batchAggregate(event, tag)

        if (buf.isFull) {
          batchFlush(tag, false)
        }

      case Flush(tag, timeout) => /* all flush start here */
        batchFlush(tag, timeout)

      case Close(closed) =>
        batchFlush(closed)
    }
  }

  @volatile final private[sink] var batchManager: ActorRef = _
  final def batchManagerRef: ActorRef = batchManager
  final def batchManagerName: String = s"actor.sink.${getName}.batchManager"

  @volatile final var batchSize: Int = _
  @volatile final var batchTimeout: FiniteDuration = _
  @volatile final var batchDispatcher: String = _

  @volatile final private var batchScheduler: Scheduler = _
  @volatile final private var batchExecutionContext: ExecutionContext = _

  final def getBatchDispatcher: String = batchDispatcher
  final def getBatchExecutionContext: ExecutionContext = batchExecutionContext

  override def bootstrap()(implicit system: ActorSystem): Unit = {
    super.bootstrap()

    val cfg = getConfiguration
    batchSize = cfg.getInt(Opts.OPT_BATCH_SIZE, Opts.DEF_BATCH_SIZE)
    batchTimeout = cfg.getDuration(Opts.OPT_BATCH_TIMEOUT,
                                   Opts.DEF_BATCH_TIMEOUT)
    batchDispatcher = cfg.getString(Opts.OPT_BATCH_AKKA_DISPATCHER,
                                    Opts.DEF_BATCH_AKKA_DISPATCHER)

    if (batchSize <= 0 && batchTimeout.length == 0) {
      throw new SinkInitializeException(
        s"cannot initialize batch manager with both infinite size and " +
        s"infinite timeout, you may want change the settings of " +
        s"${Opts.OPT_BATCH_SIZE} or ${Opts.DEF_BATCH_SIZE} to nonzero value")
    }
    if (batchSize == 1) {
      throw new SinkInitializeException(
        s"cannot initialize batch manager with batch size 1, which is " +
        s"meaningless you may want change the setting of " +
        s"${Opts.OPT_BATCH_SIZE} greater than 1")
    }
    batchManager = system.actorOf(
      BatchManagerActor.props().withDispatcher(batchDispatcher),
      batchManagerName)
    batchScheduler = system.scheduler
    batchExecutionContext = system.dispatchers.lookup(batchDispatcher)
  }

  final private[sink] val buffers =
    new JCHashMap[String, FlushableBatchBuffer]()

  final private[sink] def batchAggregate(event: Event,
                                         tag: String): FlushableBatchBuffer =
    buffers.compute(tag, new JBiFunc[String, FlushableBatchBuffer,
                                             FlushableBatchBuffer] {
      override def apply( /* key == tag, we use key inside apply */
        key: String,
        value: FlushableBatchBuffer): FlushableBatchBuffer = {
        if (value == null) {
          val buf = BatchBuffer(batchSize, event)
          val flushTask = {
            if (batchTimeout.length > 0) {
              val task = batchScheduler.scheduleOnce(
                batchTimeout, batchManager, Flush(key, true))(
                batchExecutionContext)
              Some(task)
            } else None
          }
          FlushableBatchBuffer(buf, flushTask)
        } else {
          value.append(event)
          value
        }
      }
    })

  final private[sink] def batchFlush(tag: String, timeout: Boolean): Unit = {
    val fb = buffers.remove(tag)
    if (fb == null) { /* batch already flushed */
      if (timeout) {
        logger.debug("timeout but batch (tag: <{}>) " +
                     "already flushed, ignored", tag)
      } else {
        logger.debug("batch (tag: <{}>) was full or sink being closed, " +
                     "but batch already flushed, ignored", tag)
      }
    } else {
      if (!timeout) { /* we should cancel scheduled task when batch was full */
        if (fb.cancel) {
          logger.debug("batch (tag: <{}>) full, flush task " +
                       "cancelled, flushing now", tag)
        } else {
          logger.debug("batch (tag: <{}>) full, flush task " +
                       "cancel failed, ignored", tag)
        }
      }
      ref ! fb.asBatch(tag)
    }
  }

  final private[sink] def batchFlush(closed: Promise[Closed]): Unit = {
    if (buffers.isEmpty()) {
      closed.success(Closed(getName))
    } else {
      buffers.keySet().iterator().asScala
             .foreach(tag => { batchFlush(tag, false) })
      batchManager ! Close(closed)
    }
  }

  override def close(closed: Promise[Closed]): Unit = {
    val p = Promise[Closed]()
    batchManager ! Close(p)

    Await.ready(p.future, Duration.Inf).value.get match {
      case Success(flushed) =>
        logger.debug("batch manager of sink <{}> flush " +
                     "all internal buffers", getName)
      case Failure(exc) =>
        logger.error(s"batch manager of sink <${getName}> can not " +
                     s"flush internal buffers, skip flush", exc)
    }
    super.close(closed)
  }

  def batchAppend(event: Event): Unit = batchManager ! event

  def batchTag(event: Event): String = DEF_BATCH_TAG
}

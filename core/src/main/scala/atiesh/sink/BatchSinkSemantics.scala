/*
 * Copyright (C) Hao Feng
 */

package atiesh.sink

// java
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean
// scala
import scala.util.{ Success, Failure }
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Promise, Await }
// akka
import akka.actor.{ Props, Actor, ActorSystem, ActorRef, Cancellable }
import akka.event.LoggingReceive
// internal
import atiesh.event.Event
import atiesh.statement.{ Commit, Transaction, Close, Closed }
import atiesh.utils.{ Configuration, Logging }

trait BatchSinkSemantics extends SinkSemantics with Logging { this: Sink =>
  object BatchSemanticsOpts {
    val OPT_BATCH_SIZE = "batch-size"
    val DEF_BATCH_SIZE = 0                            /* 0 meanes infinite size */
    val OPT_BATCH_TIMEOUT = "batch-timeout"
    val DEF_BATCH_TIMEOUT = Duration(0, MILLISECONDS) /* 0 meanes infinite timeout */
    val OPT_BATCHSINK_AKKA_DISPATCHER = "batch-akka-dispatcher"
    val DEF_BATCHSINK_AKKA_DISPATCHER = "akka.actor.default-dispatcher"
  }
  val DEF_BATCH_CATEGORY = "default"

  object BatchManagerActor {
    def props(): Props = Props(new BatchManagerActor())
  }
  class BatchManagerActor extends Actor with Logging {
    implicit val system = context.system
    implicit val actionSlots = new Semaphore(1)

    val currentBatchs = mutable.Map[String, Batch]()
    override def receive: Receive = LoggingReceive {
      case AppendEvent(category, event) =>
        val current = currentBatchs.getOrElseUpdate(category, Batch(category, batchSize, batchTimeout, batchAction(category)))
        val batch = current.append(event)
        if (current != batch) currentBatchs(category) = batch

      case FlushEvent(flushed) =>
        currentBatchs.foldLeft(())({ case (_, (k, v)) =>
          logger.debug("<Batch.{}> flusing batch of <{}> before close", getName, k)
          v.flush(false)
        })
        flushed.success(Closed(s"Batch.${getName}"))
    }
  }
  case class AppendEvent(batchKey: String, evnet: Event)
  case class FlushEvent(flushed: Promise[Closed])

  var batchManager: ActorRef = _
  var batchSize: Int = _
  var batchTimeout: FiniteDuration = _
  var batchExecutionContext: ExecutionContext = _

  object Batch extends Logging {
    def apply(
      category: String,
      size: Int,
      timeout: FiniteDuration,
      action: List[Event] => Unit)(implicit system: ActorSystem, slots: Semaphore): Batch = {

      logger.debug(
        "creating new batch with size <{}> and timeout <{}>", size, timeout)
      new Batch(category, size, timeout, action, None)
    }

    def apply(
      category: String,
      size: Int,
      timeout: FiniteDuration,
      action: List[Event] => Unit,
      event: Event)(implicit system: ActorSystem, slots: Semaphore): Batch = {

      logger.debug(
        "creating new batch with initial event <{}> and size <{}> and timeout <{}>", event.getBody, size, timeout)
      new Batch(category, size, timeout, action, Some(event))
    }
  }

  class Batch(
    category: String,
    size: Int,
    timeout: FiniteDuration,
    action: List[Event] => Unit,
    initial: Option[Event])(implicit system: ActorSystem, slots: Semaphore) extends Logging {

    val op = new Semaphore(1)
    val done = new AtomicBoolean(false)
    val buffer = initial match {
      case Some(event) =>
        mutable.ArrayBuffer[Event](event)
      case None =>
        mutable.ArrayBuffer[Event]()
    }

    val flushTask: Option[Cancellable] = {
      if (timeout.length > 0) {
        Some(system.scheduler.scheduleOnce(timeout)(onTimeout)(system.dispatcher))
      } else {
        None
      }
    }

    def append(event: Event): Batch = {
      /*
       * remember, we are inside the BatchManager actor, which means this method
       *  is single threaded and the <op> semaphore can be acquired under two circumstances:
       *    1, right now right here, inside the append method (protect this append from the flush task)
       *    2, flush task is already running (which means you need a new Batch for this event)
       */
      if (done.get() || !op.tryAcquire()) {
        Batch(category, size, timeout, action, event)
      } else {
        buffer.append(event)
        op.release()

        if (size > 0 && buffer.length >= size) {
          flush(false)
          Batch(category, size, timeout, batchAction(category))
        } else {
          this
        }
      }
    }

    def flush(isTimeout: Boolean): Unit = {
      if (done.compareAndSet(false, true)) {
        op.acquire()
        if (flushTask.nonEmpty && !isTimeout) {
          val task = flushTask.get
          if (task.cancel()) logger.debug("batch full, cancel batch timeout flush task")
          else logger.warn("can not cancel batch timeout flush task when batch was full, ignore")
        }
        if (buffer.length <= 0) {
          logger.debug("got empty batch, skip this action")
        } else {
          logger.debug("acquire action slot to flush batch")
          slots.acquire()
          action(buffer.toList)
          slots.release()
          logger.debug("batch flush finished, slot became idle")
        }
      }
    }

    def onTimeout(): Unit = {
      flush(true)
    }
  }

  override def stop(closed: Promise[Closed]): Unit = {
    val f = Promise[Closed]()
    batchManager ! FlushEvent(f)
    Await.ready(f.future, Duration.Inf).value.get match {
      case Success(flushed) =>
        logger.debug("<{}> flushed all batchs", flushed.component)
      case Failure(exc) =>
        logger.error(s"batch manager of sink <${getName}> can not flush pending batchs, skip flush", exc)
    }
    super.stop(closed)
  }

  override def bootstrap()(implicit system: ActorSystem): Sink = {
    super.bootstrap()

    val cfg = getConfiguration
    val akkaDispatcher = cfg.getString(
      BatchSemanticsOpts.OPT_BATCHSINK_AKKA_DISPATCHER,
      BatchSemanticsOpts.DEF_BATCHSINK_AKKA_DISPATCHER)

    batchSize = cfg.getInt(BatchSemanticsOpts.OPT_BATCH_SIZE, BatchSemanticsOpts.DEF_BATCH_SIZE)
    batchTimeout = cfg.getDuration(BatchSemanticsOpts.OPT_BATCH_TIMEOUT, BatchSemanticsOpts.DEF_BATCH_TIMEOUT)

    if (batchSize <= 0 && batchTimeout.length == 0) {
      throw new SinkInitializeException(
        s"cannot initialize batch manager with both infinite size and infinite timeout " +
        s"you may want change the setting of ${BatchSemanticsOpts.OPT_BATCH_SIZE} or " +
        s"${BatchSemanticsOpts.DEF_BATCH_SIZE} to nonzero value")
    }
    if (batchSize == 1) {
      throw new SinkInitializeException(
        s"cannot initialize batch manager with batch size is 1 which is meaningless " +
        s"you may want change the setting of ${BatchSemanticsOpts.OPT_BATCH_SIZE} greater than 1")
    }
    batchManager = system.actorOf(BatchManagerActor.props().withDispatcher(akkaDispatcher), s"${getName}.batchManager")
    batchExecutionContext = system.dispatchers.lookup(akkaDispatcher)

    this
  }

  def batchAppend(event: Event): Unit = batchAppend(DEF_BATCH_CATEGORY, event)
  def batchAction(events: List[Event]): Unit

  def batchAppend(category: String, event: Event): Unit = batchManager ! AppendEvent(category, event)
  def batchAction(category: String): List[Event] => Unit = batchAction
}

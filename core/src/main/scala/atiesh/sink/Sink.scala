/*
 * Copyright (C) Hao Feng
 */

package atiesh.sink

// scala
import scala.util.{ Try, Success, Failure }
import scala.concurrent.{ ExecutionContext, Promise, Future, Await }
import scala.concurrent.duration._
// akka
import akka.actor.{ Props, Actor, ActorRef, ActorSystem }
import akka.event.LoggingReceive
// internal
import atiesh.event.Event
import atiesh.component.ExtendedComponent
import atiesh.statement.{ Open, Ready, Close, Closed,
                          Batch, Signal, Commit, Transaction }
import atiesh.utils.{ Configuration, ComponentLoader, Logging }

trait SinkType {
  final val componentType = "sink"
}

object Sink extends SinkType with Logging {
  object SinkOpts {
    val OPT_FQCN = "fqcn"
    val OPT_AKKA_DISPATCHER = "akka-dispatcher"
    val DEF_AKKA_DISPATCHER = "akka.actor.default-dispatcher"
  }

  private[atiesh] def initializeComponents(
    scfs: Configuration)(implicit system: ActorSystem): Try[List[Sink]] =
    scfs.getSections()
      /* read & parse configuration sections */
      .foldLeft(List[(String, Configuration)]())((cfgs, sn) => {
        scfs.getSection(sn).map(scf => {
          logger.debug("readed configuration section <{}> with content " +
                       "<{}> for initialize sink(s)", sn, scf)
          (sn -> scf) :: cfgs
        }).getOrElse(cfgs)
      })
      /* load & initialize */
      .foldLeft(List[Try[Sink]]())({
        case (comps, (name, cfg)) => initializeComponent(name, cfg) :: comps
      })
      /* inspect */
      .foldLeft(Try(List[Sink]()))((sinks, sink) => {
        sinks.flatMap(sks => {
          sink match {
            case Success(s) => Success(s :: sks)
            case Failure(exc) => Failure(exc)
          }
        })
      })

  private[sink] def initializeComponent(
    name: String,
    scf: Configuration)(implicit system: ActorSystem): Try[Sink] = Try {
    val fqcn = scf.getString(SinkOpts.OPT_FQCN)
    val dispatcher = scf.getString(SinkOpts.OPT_AKKA_DISPATCHER,
                                   SinkOpts.DEF_AKKA_DISPATCHER)

    logger.debug("loading sink {} (class: {}) with configuration: {}",
                 name, fqcn, scf)
    val sink = ComponentLoader.createInstanceFor[Sink](
      fqcn, List[(Class[_], AnyRef)](
        (classOf[String], name),
        (classOf[String], dispatcher),
        (classOf[Configuration], scf)
      ))
    sink.bootstrap()

    logger.info("loaded sink {} (class: {}) with configuration: {}",
                sink.getName, fqcn, sink.getConfiguration)
    sink
  }
}

/**
 * Atiesh semantics which represent a sink component.
 */
trait SinkSemantics
  extends Logging { this: ExtendedComponent with SinkMetrics =>
  object SinkActor {
    /*
     * Inner class SinkActor class are different class in each
     * Sink instances, we need create them inside each instance,
     * otherwise you may got unstable identifier issues
     */ 
    def props(): Props = Props(new SinkActor())
  }
  final private class SinkActor extends Actor with Logging {
    override def receive: Receive = LoggingReceive {
      case event: Event =>
        metricsSinkEventAcceptedCounter.increment()
        metricsSinkComponentEventAcceptedCounter.increment()

        process(event)

      case Batch(events, aggregatedBy) =>
        metricsSinkBatchAcceptedCounter.increment()
        metricsSinkComponentBatchAcceptedCounter.increment()

        process(events, aggregatedBy)

      case Signal(sig) =>
        metricsSinkSignalAcceptedCounter.increment()
        metricsSinkComponentSignalAcceptedCounter.increment()

        process(sig)

      case Commit(tran) =>
        ack(tran)

      case Open(ready) =>
        try {
          open(ready)
        } catch {
          case exc: Throwable =>
            ready.failure(exc)
        }

      case Close(closed) =>
        try {
          close(closed)
        } catch {
          case exc: Throwable =>
            closed.failure(exc)
        }
    }
  }
  @volatile final private[sink] var ref: ActorRef = _
  final def actorRef: ActorRef = ref
  final def actorName: String = s"actor.sink.${getName}"

  def bootstrap()(implicit system: ActorSystem): Unit = {
    ref = system.actorOf(SinkActor.props().withDispatcher(getDispatcher),
                         actorName)
  }

  final def signal(sig: Int): Unit = {
    logger.debug("sink <{}> triggering signal <{}>", getName, sig)
    ref ! Signal(sig)
  }

  final def submit(event: Event): Unit = {
    logger.debug("sink <{}> submitting event <{}>", getName, event.getBody)
    ref ! event
  }

  /**
   * Current not used, we never send <Batch> from souce to sink.
   * final def submit(events: List[Event]): Unit = {
   *   logger.debug("sink <{}> submitting events <{} ({} events)>", getName,
   *                events.headOption.map(_.getBody).getOrElse("-"), events.length)
   *   ref ! Batch(events, getName)
   * }
   */

  final def commit(tran: Promise[Transaction]): Future[Transaction] = {
    logger.debug("sink <{}> triggering commit transaction statement <{}@{}>",
                 getName, tran, tran.hashCode.toHexString)
    ref ! Commit(tran)
    tran.future
  }

  def open(ready: Promise[Ready]): Unit = {
    startup()
    metricsComponentStartTimestampGauge.update(System.currentTimeMillis)
    ready.success(Ready(getName))
  }

  def close(closed: Promise[Closed]): Unit = {
    shutdown()
    closed.success(Closed(getName))
  }

  def ack(tran: Promise[Transaction]): Unit = {
    logger.debug(
      "sink <{}> acknowledging commit transaction statement <{}@{}>",
      getName, tran, tran.hashCode.toHexString)
    tran.success(Transaction(getName))
  }

  def process(sig: Int): Unit =
    logger.debug("sink <{}> handling signal <{}>", getName, sig)

  def process(events: List[Event], aggregatedBy: String): Unit = {
    logger.debug("sink <{}> handling batch <{}>", getName, aggregatedBy)
    events.foreach(event => process(event))
  }

  /**
   * Atiesh sink component api (high level api).
   */
  def accept(event: Event): Boolean
  def process(event: Event): Unit
}

/**
 * Atiesh sink component.
 */
trait Sink
  extends ExtendedComponent
  with SinkType
  with SinkMetrics
  with SinkSemantics {
  final protected[this] var sec: ExecutionContext = _

  /*
   * use the volatile mark variable <ref> inside SinkSemantics as 
   * memory barrier, which insert a <MFENCE> that make all changes
   * of the memory synced before the server invoke start, to achieve
   * this we should invoke <super.bootstrap> after we setup everything
   */
  override def bootstrap()(implicit system: ActorSystem): Unit = {
    sec = system.dispatchers.lookup(getDispatcher)
    super.bootstrap()
  }

  def start(ready: Promise[Ready]): Future[Ready] = {
    logger.debug("sink <{}> triggering ready statement <{}@{}>",
                 getName, ready, ready.hashCode.toHexString)
    ref ! Open(ready)
    ready.future
  }

  def stop(closed: Promise[Closed]): Future[Closed] = {
    logger.debug("sink <{}> triggering closed statement <{}@{}>",
                 getName, closed, closed.hashCode.toHexString)
    ref ! Close(closed)
    closed.future
  }
}

/**
 * Atiesh sink component with constructor.
 */
abstract class AtieshSink(name: String,
                          dispatcher: String,
                          cfg: Configuration)
  extends Sink {
  final def getName: String = name
  final def getDispatcher: String = dispatcher
  final def getConfiguration: Configuration = cfg
  final def getExecutionContext: ExecutionContext = sec
}

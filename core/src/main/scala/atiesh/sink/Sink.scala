/*
 * Copyright (C) Hao Feng
 */

package atiesh.sink

// scala
import scala.util.{ Try, Success, Failure }
import scala.concurrent.{ Promise, Future, Await }
import scala.concurrent.duration._
// akka
import akka.actor.{ Props, Actor, ActorSystem, ActorRef }
import akka.event.LoggingReceive
// internal
import atiesh.event.Event
import atiesh.statement.{ Signal, Open, Ready, Close, Closed, Commit, Transaction }
import atiesh.utils.{ Configuration, ClassLoader, Logging }
import atiesh.metrics.MetricsGroup._

trait Sink {
  def getName: String
  def getDispatcher: String
  def getConfiguration: Configuration

  def bootstrap()(implicit system: ActorSystem): Sink
  object SinkActor {
    def props(): Props = Props(new SinkActor())
  }
  class SinkActor extends Actor with Logging {
    override def receive: Receive = LoggingReceive {
      case Open(ready) =>
        try {
          startup()
          ready.success(Ready(getName))
        } catch {
          case exc: Throwable =>
            ready.failure(exc)
        }

      case event: Event =>
        process(event)

      case Signal(sig) =>
        process(sig)

      case Close(closed) =>
        stop(closed)

      case Commit(tran) =>
        ack(tran)
    }
  }
  var ref: ActorRef = _

  def open(ready: Promise[Ready]): Future[Ready]
  def stop(closed: Promise[Closed]): Unit
  def close(closed: Promise[Closed]): Future[Closed]

  def submit(event: Event): Unit
  def commit(tran: Promise[Transaction]): Future[Transaction]
  def ack(tran: Promise[Transaction]): Unit

  def accept(event: Event): Boolean
  def process(event: Event): Unit

  def signal(sig: Int): Unit
  def process(sig: Int): Unit

  def startup(): Unit
  def shutdown(): Unit
}

trait SinkSemantics extends Logging { this: Sink =>
  def actorName: String = s"actor.sink.${getName}"

  def open(ready: Promise[Ready]): Future[Ready] = {
    ref ! Open(ready)
    ready.future
  }

  def close(closed: Promise[Closed]): Future[Closed] = {
    logger.debug("sink <{}> receiving closing statement <{}@{}>", getName, closed, closed.hashCode.toHexString)
    ref ! Close(closed)
    closed.future
  }

  def signal(sig: Int): Unit = {
    SinkMetrics.signalSendCount.increment()
    logger.debug("sink <{}> sending signal <{}>", getName, sig)
    ref ! Signal(sig)
  }

  def submit(event: Event): Unit = {
    SinkMetrics.eventSubmitCount.increment()
    ref ! event
  }

  def commit(tran: Promise[Transaction]): Future[Transaction] = {
    SinkMetrics.transactionCommitCount.increment()
    logger.debug("sink <{}> receiving commit statement <{}@{}>", getName, tran, tran.hashCode.toHexString)
    ref ! Commit(tran)
    tran.future
  }

  def ack(tran: Promise[Transaction]): Unit = {
    SinkMetrics.transactionAckedCount.increment()
    logger.debug("sink <{}> acknowledging commit statement <{}@{}>", getName, tran, tran.hashCode.toHexString)
    tran.success(Transaction(getName))
  }

  def stop(closed: Promise[Closed]): Unit = {
    try {
      shutdown()
    } catch {
      case exc: Throwable =>
        closed.failure(exc)
    }
    if (!closed.isCompleted) closed.success(Closed(getName))
  }

  def bootstrap()(implicit system: ActorSystem): Sink = {
    ref = system.actorOf(SinkActor.props().withDispatcher(getDispatcher), actorName)
    this
  }
}

abstract class AtieshSink(name: String, dispatcher: String, cfg: Configuration) extends Sink with SinkSemantics {
  final def getName: String = name
  final def getDispatcher: String = dispatcher
  final def getConfiguration: Configuration = cfg
}

object Sink extends Logging {
  object ComponentOpts {
    val OPT_SINK_CLSNAME = "type"
    val OPT_AKKA_DISPATCHER = "akka-dispatcher"
    val DEF_AKKA_DISPATCHER = "akka.actor.default-blocking-io-dispatcher"
  }
  def getComponentName: String = "sinks"

  def initializeComponents(scf: Configuration)(implicit system: ActorSystem): Try[List[Sink]] = {
    val scfs = scf.getSections()
      .foldLeft(List[(String, Configuration)]())((cfgs, sn) => {
        scf.getSection(sn).map(sc => {
          logger.debug(
            "readed configuration section <{}> with content <{}> for initialize sink(s)", sn, sc)
          (sn -> sc) :: cfgs
        }).getOrElse(cfgs)
      })

    val comps = scfs.foldLeft(List[Try[Sink]]())({ case (comps, (name, cfg)) => initializeComponent(name, cfg) :: comps })

    comps.foldLeft(Try(List[Sink]()))((sinks, sink) => {
      sinks.flatMap(sks => {
        sink match {
          case Success(s) => Success(s :: sks)
          case Failure(exc) => Failure(exc)
        }
      })
    })
  }

  def initializeComponent(name: String, scf: Configuration)(implicit system: ActorSystem): Try[Sink] = Try {
    val className = scf.getString(ComponentOpts.OPT_SINK_CLSNAME)
    val dispatcher = scf.getString(ComponentOpts.OPT_AKKA_DISPATCHER, ComponentOpts.DEF_AKKA_DISPATCHER)

    logger.debug("loading sink {} (class: {}) with configuration: {}", name, className, scf)
    val sink = ClassLoader.loadClassInstanceByName[Sink](
      className, List[(AnyRef, Class[_])](
        (name, classOf[String]),
        (dispatcher, classOf[String]),
        (scf, classOf[Configuration])
      )).bootstrap()
    logger.info("loaded sink {} (class: {}) with configuration: {}", sink.getName, className, sink.getConfiguration)

    sink
  }

  def shutdownComponents(sinks: List[Sink]): Unit = {
    sinks.map(sink => {
      val p = Promise[Closed]()
      logger.info("shutting down sink component, shutting down <{}>", sink.getName)
      (sink -> sink.close(p))
    }).foreach({ case (sink, closedFuture) =>
      Await.ready(closedFuture, Duration.Inf).value.get match {
        case Success(closed) =>
          logger.info("atiesh sink <{}> closed", closed.component)
        case Failure(exc) =>
          logger.info("atiesh sink <{}> close failed", sink.getName)
      }
    })
  }
}

/*
 * Copyright (C) Hao Feng
 */

package atiesh.source

// java
import java.util.concurrent.atomic.AtomicBoolean
// scala
import scala.concurrent.{ Promise, Future, Await }
import scala.util.{ Try, Success, Failure }
import scala.collection.mutable.HashSet
import scala.concurrent.duration._
// akka
import akka.actor.{ Props, Actor, ActorSystem, ActorRef }
import akka.event.LoggingReceive
// internal
import atiesh.event.{ Event, Empty }
import atiesh.statement.{ Open, Ready, Close, Closed, Continue, Commit, Transaction }
import atiesh.sink.Sink
import atiesh.interceptor.Interceptor
import atiesh.utils.{ Configuration, ClassLoader, Logging }
import atiesh.metrics.MetricsGroup._

trait Source {
  def getName: String
  def getDispatcher: String
  def getConfiguration: Configuration
  def getInterceptors: List[Interceptor]
  def getSinks: List[Sink]
  def getSinkSelectStrategy: String

  val shuttingDown = new AtomicBoolean(false)

  def scheduleNextCycle(): Unit = ref ! Continue

  def bootstrap()(implicit system: ActorSystem): Source
  object SourceActor {
    def props(): Props = Props(new SourceActor())
  }
  class SourceActor extends Actor with Logging {
    override def receive: Receive = LoggingReceive {
      case Open(ready) =>
        try {
          startup()
          ready.success(Ready(getName))
          scheduleNextCycle()
        } catch {
          case exc: Throwable =>
            ready.failure(exc)
        }

      case Close(closed) =>
        if (shuttingDown.compareAndSet(false, true)) stop(closed)

      case Continue =>
        if (!shuttingDown.get()) {
          mainCycle()
          SourceMetrics.cycleRunsCount.increment()
          scheduleNextCycle()
        }
    }
  }
  var ref: ActorRef = _

  def open(ready: Promise[Ready]): Future[Ready]
  def stop(closed: Promise[Closed]): Unit
  def close(closed: Promise[Closed]): Future[Closed]

  def intercept(event: Event): Event
  def sink(event: Event): Unit

  def startup(): Unit
  def mainCycle(): Unit
  def shutdown(): Unit
}

trait SourceSemantics extends Logging { this: Source =>
  def actorName: String = s"actor.source.${getName}"

  def open(ready: Promise[Ready]): Future[Ready] = {
    ref ! Open(ready)
    ready.future
  }

  def close(closed: Promise[Closed]): Future[Closed] = {
    logger.debug("source <{}> receiving closing statement <{}@{}>", getName, closed, closed.hashCode.toHexString)
    ref ! Close(closed)
    closed.future
  }

  def intercept(event: Event): Event = {
    getInterceptors.foldLeft(event)((interceptedEvent, interceptor) => {
      interceptedEvent match {
        case Empty =>
          Empty

        case event: Event =>
          try {
            interceptor.intercept(event) match {
              case Empty =>
                InterceptorMetrics.eventDiscardedCount.increment()
                Empty
              case e: Event => e
            }
          } catch {
            case exc: Throwable =>
              logger.error(
                s"got unexpected exception inside interceptor " +
                s"<${interceptor.getName}>, with event <${event.getBody}>", exc)
              InterceptorMetrics.eventInterceptFailedCount.increment()
              event
          }
      }
    })
  }

  import Source.SinkSelectStrategies
  val transactions = HashSet[Sink]()
  def sink(event: Event): Unit = {
    val pipe = {
      if (getSinks.length == 1 &&
        getSinkSelectStrategy == SinkSelectStrategies.SELECT_STRATEGY_SKIP_ACCEPT_CHECK_ON_SINGLE) {
        logger.debug("only one sink connected and <skip-accept-check-on-single> set, bypass accept check for sink")
        Some(getSinks.head)
      } else {
        getSinks.find(_.accept(event))
      }
    }

    pipe match {
      case None =>
        logger.warn("no sink can consume event <{}>, ignore and discard this event", event.getBody)
      case Some(sk) =>
        if (transactions.add(sk)) {
          logger.debug(
            "append sink <{}> to transactions set <{}>",
            sk.getName,
            transactions.foldLeft(List[String]())((ss, s) => { s.getName :: ss }).mkString(","))
        }
        sk.submit(event)
    }
  }

  def commit(): Unit = {
    logger.debug(
      "source <{}> trying to commit sink transactions <{}>",
      getName,
      transactions.foldLeft(List[String]())((ss, s) => { s.getName :: ss }).mkString(","))

    transactions.foldLeft(List[(Sink, Future[Transaction])]())((trans, s) => {
      (s -> s.commit(Promise[Transaction]())) :: trans
    }).foreach({ case (sk, tx) =>
      Await.ready(tx, Duration.Inf).value.get match {
        case Success(Transaction(owner)) =>
          logger.debug("downstream sink <{}> commit successed", owner)

        case Failure(exc) =>
          logger.error(s"downstream sink <${sk.getName}> transaction commit failed, " +
            "which means you may use a sink with wrong semantics implementation",
            exc)
      }
    })
    transactions.clear()
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

  def bootstrap()(implicit system: ActorSystem): Source = {
    ref = system.actorOf(SourceActor.props().withDispatcher(getDispatcher), actorName)
    this
  }
}


abstract class AtieshSource(
  name: String,
  dispatcher: String,
  cfg: Configuration,
  interceptors: List[Interceptor],
  sinks: List[Sink],
  strategy: String) extends Source with SourceSemantics {

  def getName: String = name
  def getDispatcher: String = dispatcher
  def getConfiguration: Configuration = cfg
  def getInterceptors: List[Interceptor] = interceptors
  def getSinks: List[Sink] = sinks
  def getSinkSelectStrategy: String = strategy
}

object Source extends Logging {
  object SinkSelectStrategies {
    val SELECT_STRATEGY_FIRST_ACCEPTED = "first-accepted"
    val SELECT_STRATEGY_SKIP_ACCEPT_CHECK_ON_SINGLE = "skip-accept-check-on-single"

    val DEFAULT_SELECT_STRATEGY = SELECT_STRATEGY_SKIP_ACCEPT_CHECK_ON_SINGLE

    def isValidStrategy(strategy: String): Boolean = {
      strategy == SELECT_STRATEGY_SKIP_ACCEPT_CHECK_ON_SINGLE ||
        strategy == SELECT_STRATEGY_FIRST_ACCEPTED
    }
  }

  object ComponentOpts {
    val OPT_SOURCE_CLSNAME = "type"
    val OPT_INTERCEPTORS = "interceptors"
    val DEF_INTERCEPTORS = List[String]()
    val OPT_SINKS = "sinks"
    val DEF_SINKS = List[String]()
    val OPT_SINK_SELECT_STRATEGY = "sink-select-strategy"
    val DEF_SINK_SELECT_STRATEGY = SinkSelectStrategies.DEFAULT_SELECT_STRATEGY
    val OPT_AKKA_DISPATCHER = "akka-dispatcher"
    val DEF_AKKA_DISPATCHER = "akka.actor.default-blocking-io-dispatcher"
  }
  def getComponentName: String = "sources"

  def initializeComponents(
    scf: Configuration,
    interceptors: List[Interceptor],
    sinks: List[Sink])(implicit system: ActorSystem): Try[List[Source]] = {

    val scfs = scf.getSections()
      .foldLeft(List[(String, Configuration)]())((cfgs, sn) => {
        scf.getSection(sn).map(sc => {
          logger.debug(
            "readed configuration section <{}> with content <{}> for initialize source(s)", sn, sc)
          (sn -> sc) :: cfgs
        }).getOrElse(cfgs)
      })

    val comps = scfs.foldLeft(List[Try[Source]]())({ case (comps, (name, cfg)) =>
      val activeInterceptors = cfg.getStringList(ComponentOpts.OPT_INTERCEPTORS, ComponentOpts.DEF_INTERCEPTORS)
        .foldLeft(List[Interceptor]())((used, interceptorName) => {
          interceptors.find(_.getName == interceptorName) match {
            case Some(i) =>
              i :: used
            case None =>
              logger.warn(
                "configurated interceptor <{}> not found during source <{}> initialize, ignore",
                interceptorName,
                name)
              used
          }
        }).sortBy(-_.getPriority)

      val activeSinks = cfg.getStringList(ComponentOpts.OPT_SINKS, ComponentOpts.DEF_SINKS)
        .foldLeft(List[Sink]())((used, sinkName) => {
          sinks.find(_.getName == sinkName) match {
            case Some(s) =>
              s :: used
            case None =>
              logger.warn(
                "configurated sink <{}> not found during source <{}> initialize, ignore",
                sinkName,
                name)
              used
          }
        }).reverse // make sure the sinks ordered as same as they configured inside the configuration file

      val sinkSelectStrategy = {
        val strategy = cfg.getString(ComponentOpts.OPT_SINK_SELECT_STRATEGY, ComponentOpts.DEF_SINK_SELECT_STRATEGY)
        if (!SinkSelectStrategies.isValidStrategy(strategy)) {
          logger.warn(
            "configurated sink select strategy <{}> not valid during source <{}> initialize, ignore",
            strategy,
            name)
          SinkSelectStrategies.DEFAULT_SELECT_STRATEGY
        } else {
          strategy
        }
      }

      initializeComponent(name, cfg, activeInterceptors, activeSinks, sinkSelectStrategy) :: comps
    })

    comps.foldLeft(Try(List[Source]()))((sources, source) => {
      sources.flatMap(ss => {
        source match {
          case Success(s) => Success(s :: ss)
          case Failure(exc) => Failure(exc)
        }
      })
    })
  }

  def initializeComponent(
    name: String,
    scf: Configuration,
    interceptors: List[Interceptor],
    sinks: List[Sink],
    strategy: String)(implicit system: ActorSystem): Try[Source] = Try {

    val className = scf.getString(ComponentOpts.OPT_SOURCE_CLSNAME)
    val dispatcher = scf.getString(ComponentOpts.OPT_AKKA_DISPATCHER, ComponentOpts.DEF_AKKA_DISPATCHER)

    logger.debug("loading source {} (class: {}) with configuration: {}", name, className, scf)
    val source = ClassLoader.loadClassInstanceByName[Source](
      className, List[(AnyRef, Class[_])](
        (name, classOf[String]),
        (dispatcher, classOf[String]),
        (scf, classOf[Configuration]),
        (interceptors, classOf[List[Interceptor]]),
        (sinks, classOf[List[Sink]]),
        (strategy, classOf[String])
      )).bootstrap()
    logger.info("loaded source {} (class: {}) with configuration: {}", source.getName, className, source.getConfiguration)

    source
  }

  def shutdownComponents(sources: List[Source]): Unit = {
    sources.map(source => {
      val p = Promise[Closed]()
      logger.info("shutting down source component, shutting down <{}>", source.getName)
      (source -> source.close(p))
    }).foreach({ case (source, closedFuture) =>
      Await.ready(closedFuture, Duration.Inf).value.get match {
        case Success(closed) =>
          logger.info("atiesh source <{}> closed", closed.component)
        case Failure(exc) =>
          logger.info("atiesh source <{}> close failed", source.getName)
      }
    })
  }
}

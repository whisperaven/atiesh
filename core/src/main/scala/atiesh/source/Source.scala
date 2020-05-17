/*
 * Copyright (C) Hao Feng
 */

package atiesh.source

// java
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ ConcurrentHashMap => JCHashMap }
// scala
import scala.concurrent.{ ExecutionContext, Promise, Future, Await }
import scala.util.{ Try, Success, Failure }
import scala.concurrent.duration._
import scala.collection.JavaConverters._
// akka
import akka.actor.{ Props, Actor, ActorSystem, ActorRef }
import akka.event.LoggingReceive
// internal
import atiesh.event.{ Event, Empty }
import atiesh.statement.{ Open, Ready, Close, Closed, Continue, Signal,
                          Offer, Confirmation, Commit, Transaction }
import atiesh.component.ExtendedComponent
import atiesh.sink.Sink
import atiesh.interceptor.Interceptor
import atiesh.utils.{ Configuration, ComponentLoader, Logging }

trait SourceType {
  final val componentType = "source"
}

object Source extends SourceType with Logging {
  object SourceOpts {
    val OPT_FQCN = "fqcn"
    val OPT_INTERCEPTORS = "interceptors"
    val DEF_INTERCEPTORS = List[String]()
    val OPT_SINKS = "sinks"
    val DEF_SINKS = List[String]()
    val OPT_AKKA_DISPATCHER = "akka-dispatcher"
    val DEF_AKKA_DISPATCHER = "akka.actor.default-blocking-io-dispatcher"

    import SinkSelectStrategies._
    val OPT_SINK_SELECT_STRATEGY = "sink-select-strategy"
    val DEF_SINK_SELECT_STRATEGY = DEFAULT_SELECT_STRATEGY
  }

  object SinkSelectStrategies {
    val FIRST_ACCEPTED = "first-accepted"
    val SKIP_ACCEPT_CHECK_ON_SINGLE = "skip-accept-check-on-single"

    val DEFAULT_SELECT_STRATEGY = SKIP_ACCEPT_CHECK_ON_SINGLE

    def isValidStrategy(strategy: String): Boolean = {
      strategy == SKIP_ACCEPT_CHECK_ON_SINGLE ||
        strategy == FIRST_ACCEPTED
    }
  }

  private[atiesh] def initializeComponents(
    scfs: Configuration,
    interceptors: List[Interceptor],
    sinks: List[Sink])(implicit system: ActorSystem): Try[List[Source]] =
    scfs.getSections()
      /* read & parse configuration sections */
      .foldLeft(List[(String, Configuration)]())((cfgs, sn) => {
        scfs.getSection(sn).map(scf => {
          logger.debug("readed configuration section <{}> with content " +
                       "<{}> for initialize source(s)", sn, scf)
          (sn -> scf) :: cfgs
        }).getOrElse(cfgs)
      })
      /* load & initialize */
      .foldLeft(List[Try[Source]]())({
        case (comps, (name, cfg)) => {
          val ints =
            cfg.getStringList(SourceOpts.OPT_INTERCEPTORS,
                              SourceOpts.DEF_INTERCEPTORS)
              .foldLeft(List[Interceptor]())((used, interceptorName) => {
                interceptors.find(_.getName == interceptorName) match {
                  case Some(i) =>
                    i :: used
                  case None =>
                    throw new SourceInitializeException(
                      s"configurated interceptor <${interceptorName}> " +
                      s"not found during source <${name}> initialize, " +
                      s"abort initialize")
                }
              }).sortBy(-_.getPriority)
          val sks =
            cfg.getStringList(SourceOpts.OPT_SINKS, SourceOpts.DEF_SINKS)
              .foldLeft(List[Sink]())((used, sinkName) => {
                sinks.find(_.getName == sinkName) match {
                  case Some(sk) =>
                    sk :: used
                  case None =>
                    throw new SourceInitializeException(
                      s"configurated sink <${sinkName}> not found during " +
                      s"source <${name}> initialize, abort initialize")
                }
              }).reverse
          val strategy = {
            val _s = cfg.getString(SourceOpts.OPT_SINK_SELECT_STRATEGY,
                                   SourceOpts.DEF_SINK_SELECT_STRATEGY)
            if (SinkSelectStrategies.isValidStrategy(_s)) _s
            else throw new SourceInitializeException(
                   s"configurated invalid sink select strategy <${_s}> " +
                   s"was found during source <${name}> initialize, " +
                   s"abort initialize")
          }
          initializeComponent(name, cfg, ints, sks, strategy) :: comps
        }
      })
      /* inspect */
      .foldLeft(Try(List[Source]()))((sources, source) => {
        sources.flatMap(srcs => {
          source match {
            case Success(s) => Success(s :: srcs)
            case Failure(exc) => Failure(exc)
          }
        })
      })

  private[source] def initializeComponent(
    name: String,
    scf: Configuration,
    interceptors: List[Interceptor],
    sinks: List[Sink],
    strategy: String)(implicit system: ActorSystem): Try[Source] = Try {
    val fqcn = scf.getString(SourceOpts.OPT_FQCN)
    val dispatcher = scf.getString(SourceOpts.OPT_AKKA_DISPATCHER,
                                   SourceOpts.DEF_AKKA_DISPATCHER)

    logger.debug("loading source {} (class: {}) with configuration: {}",
                 name, fqcn, scf)
    val source = ComponentLoader.createInstanceFor[Source](
      fqcn, List[(Class[_], AnyRef)](
        (classOf[String], name),
        (classOf[String], dispatcher),
        (classOf[Configuration], scf),
        (classOf[List[Interceptor]], interceptors),
        (classOf[List[Sink]], sinks),
        (classOf[String], strategy)
      ))
    source.bootstrap()

    logger.info("loaded source {} (class: {}) with configuration: {}",
                source.getName, fqcn, source.getConfiguration)
    source
  }
}

/**
 * Atiesh semantics which represent a source component.
 */
trait SourceSemantics
  extends Logging { this: ExtendedComponent with SourceMetrics =>
  object SourceActor {
    /*
     * Inner class SourceActor class are different class in each
     * Source instances, we need create them inside each instance,
     * otherwise you may got unstable identifier issues
     */ 
    def props(): Props = Props(new SourceActor())
  }
  final private class SourceActor extends Actor with Logging {
    override def receive: Receive = LoggingReceive {
      case Offer(events, confirm) =>
        if (!shuttingDown.get()) {
          sourceCycle(events)
          commit()

          try {
            doneCycle()
          } catch {
            case exc: Throwable =>
              logger.error(s"passive source <${getName}> throw unexpected " +
                           s"exception inside handler doneCycle, which " +
                           s"means you may use a source component with " +
                           s"wrong implementation", exc)
          }
          confirm.success(Confirmation(getName))

          metricsSourceCycleRunCounter.increment()
          metricsSourceComponentCycleRunCounter.increment()
        } else confirm.failure(
            new IllegalStateException(s"got offer statement but source " +
                                      s"<${getName}> was shutting down"))
      case Continue =>
        if (!shuttingDown.get()) {
          try {
            sourceCycle(mainCycle())
            commit()
            doneCycle()
          } catch {
            case exc: Throwable =>
              logger.error(s"source <${getName}> throw unexpected " +
                           s"exception inside handler mainCycle/doneCycle, " +
                           s"which means you may use a source component " +
                           s"with wrong implementation", exc)
          }
          scheduleNextCycle()

          metricsSourceCycleRunCounter.increment()
          metricsSourceComponentCycleRunCounter.increment()
        } else {
          logger.warn(s"ignored continue statement, " +
                      s"source <${getName}> shutting down")
        }

      case Signal(sig) =>
        metricsSourceSignalAcceptedCounter.increment()
        metricsSourceComponentSignalAcceptedCounter.increment()

        process(sig)

      case Open(ready) =>
        try {
          open(ready)
          if (isActive) {
            scheduleNextCycle()
          }
        } catch {
          case exc: Throwable =>
            ready.failure(exc)
        }

      case Close(closed) =>
        try {
          if (shuttingDown.compareAndSet(false, true)) {
            close(closed)
          }
        } catch {
          case exc: Throwable =>
            closed.failure(exc)
        }
    }
  }
  @volatile final private[source] var ref: ActorRef = _
  final def actorRef: ActorRef = ref
  final def actorName: String = s"actor.source.${getName}"

  final private[this] val shuttingDown = new AtomicBoolean(false)
  final def isShuttingDown: Boolean = shuttingDown.get

  def bootstrap()(implicit system: ActorSystem): Unit = {
    if ((isActive && isPassive) || (!isActive && !isPassive)) {
      throw new IllegalStateException(
        s"source <${getName}> can not be both active and passive or " +
        s"either active nor passive, you may use a source component " +
        s"with wrong implementation")
    }
    ref = system.actorOf(SourceActor.props().withDispatcher(getDispatcher),
                         actorName)
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

  final def signal(sig: Int): Unit = {
    logger.debug("source <{}> triggering signal <{}>", getName, sig)
    ref ! Signal(sig)
  }

  final def intercept(event: Event): Event =
    getInterceptors.foldLeft(event)((interceptedEvent, interceptor) => {
      interceptedEvent match {
        case Empty => Empty
        case event: Event =>
          try {
            interceptor.intercept(event) match {
              case Empty => Empty
              case e: Event => e
            }
          } catch {
            case exc: Throwable =>
              logger.error(
                s"got unexpected exception inside interceptor " +
                s"<${interceptor.getName}> while try to intercept event " +
                s"body <${event.getBody}> return origin event", exc)
              metricsSourceEventInterceptErrorCounter.increment()
              metricsSourceComponentEventInterceptErrorCounter.increment()

              event
          }
      }
    }) match {
      case Empty =>
        metricsSourceEventInterceptDiscardCounter.increment()
        metricsSourceComponentEventInterceptDiscardCounter.increment()
        Empty
      case event: Event => event
    }

  import Source.SinkSelectStrategies._
  final private[this] val transactions =
    JCHashMap.newKeySet[Sink](getSinks.length)
  final def sink(event: Event): Unit = {
    if (getSinks.length == 1 &&
        getSinkSelectStrategy == SKIP_ACCEPT_CHECK_ON_SINGLE) {
      logger.debug("only one sink connected and  " +
                   "<skip-accept-check-on-single> set, " +
                   "bypass accept check for sink")
      Some(getSinks.head)
    } else {
      getSinks.find(_.accept(event))
    }
  } match {
    case None =>
      metricsSourceEventSubmitDiscardCounter.increment()
      metricsSourceComponentEventSubmitDiscardCounter.increment()
      logger.warn("no sink can consume event <{}>, ignore and discard event",
                  event.getBody)
    case Some(sk) =>
      if (transactions.add(sk)) {
        logger.debug("append sink <{}> to transactions set <{}>", sk.getName,
                     transactions.iterator().asScala
                                 .foldLeft(List[String]())((ss, s) => {
                                   s.getName :: ss
                                 }).mkString(","))
      }
      sk.submit(event)
  }

  /**
   * Current Not Used, we never send <Batch> from souce to sink.
   * def sink(events: List[Event]): Unit = {
   *   if (getSinks.length == 1 &&
   *       getSinkSelectStrategy == SKIP_ACCEPT_CHECK_ON_SINGLE) {
   *     logger.debug("only one sink connected and " +
   *                  "<skip-accept-check-on-single> set, " +
   *                  "bypass accept check for sink")
   *     Map(getSinks.head -> events)
   *   } else {
   *     events.foldLeft(mutable.Map[Sink, mutable.ArrayBuffer[Event]]())(
   *       (m, e) => {
   *         getSinks.find(_.accept(e)) match {
   *           case None =>
   *             metricsSourceSubmitDiscardCounter.increment()
   *             metricsSourceComponentSubmitDiscardCounter.increment()
   *             logger.warn("no sink can consume event <{}>, " +
   *                         "ignore and discard event", e.getBody)
   *             m
   *           case Some(sk) =>
   *             m.getOrElseUpdate(sk, mutable.ArrayBuffer[Event]()).append(e)
   *             m
   *         }
   *       })
   *       .map({
   *         case (sk, buf) => (sk -> buf.toList)
   *       }).toMap
   *   }
   * }.foreach({
   *   case (sk, es) =>
   *     if (transactions.add(sk)) {
   *       logger.debug("append sink <{}> to transactions set <{}>", sk.getName,
   *                    transactions.iterator().asScala
   *                                .foldLeft(List[String]())((ss, s) => {
   *                                  s.getName :: ss
   *                                }).mkString(","))
   *     }
   *     sk.submit(es)
   * })
   */

  final def commit(): Unit = {
    logger.debug(
      "source <{}> trying to commit sink transactions <{}>", getName,
      transactions.iterator().asScala
                  .foldLeft(List[String]())((ss, s) => {
                    s.getName :: ss
                  }).mkString(","))

    transactions.iterator().asScala
      .foldLeft(List[(Sink, Future[Transaction])]())((trans, s) => {
        (s -> s.commit(Promise[Transaction]())) :: trans
      })
      .foreach({ case (sk, tx) =>
        Await.ready(tx, Duration.Inf).value.get match {
          case Success(Transaction(_)) =>
            logger.debug("downstream sink <{}> acknowledged " +
                         "commit transaction", sk.getName)
          case Failure(exc) =>
            logger.error(s"downstream sink <${sk.getName}> transaction " +
                         s"commit failed, which means you may use a sink " +
                         s"with wrong semantics implementation", exc)
        }
      })
    transactions.clear()
  }

  final def sourceCycle(events: List[Event]): Unit = {
    events.foreach(e => {
      metricsSourceEventAcceptedCounter.increment()
      metricsSourceComponentEventAcceptedCounter.increment()

      intercept(e) match {
        case Empty =>
          logger.debug("event <{}> discarded by interceptor", e.getBody)
        case intercepted: Event =>
          sink(intercepted)
      }
    })
  }

  def process(sig: Int): Unit =
    logger.debug("source <{}> handling signal <{}>", getName, sig)

  /**
   * Atiesh source component api.
   */
  def getInterceptors: List[Interceptor]
  def getSinks: List[Sink]
  def getSinkSelectStrategy: String

  /**
   * Atiesh active source api.
   */
  final def scheduleNextCycle(): Unit = ref ! Continue
  def mainCycle(): List[Event]

  /**
   * Atiesh passive source api.
   */
  final def scheduleNextCycle(events: List[Event]): Future[Confirmation] = {
    val confirm = Promise[Confirmation]()
    ref ! Offer(events, confirm)
    confirm.future
  }

  /**
   * Atiesh active & passive source flags.
   */
  def isActive: Boolean
  def isPassive: Boolean

  /**
   * Atiesh active & passive source post cycle hook.
   */
  def doneCycle(): Unit
}

trait ActiveSourceSemantics { this: SourceSemantics =>
  final def isActive: Boolean = true
  final def isPassive: Boolean = false
}

trait PassiveSourceSemantics { this: SourceSemantics =>
  final def isActive: Boolean = false
  final def isPassive: Boolean = true

  /**
   * Passive source doesn't invoke this method, provide
   * this default one fulfill the source semantics api.
   */
  final def mainCycle(): List[Event] = List[Event]()
}

/**
 * Atiesh source component.
 */
trait Source
  extends ExtendedComponent
  with SourceType
  with SourceMetrics
  with SourceSemantics {
  final protected[this] var sec: ExecutionContext = _

  /*
   * use the volatile mark variable <ref> inside SourceSemantics as 
   * memory barrier, which insert a <MFENCE> that make all changes
   * of the memory synced before the server invoke start, to achieve
   * this we should invoke <super.bootstrap> after we setup everything
   */
  override def bootstrap()(implicit system: ActorSystem): Unit = {
    sec = system.dispatchers.lookup(getDispatcher)
    super.bootstrap()
  }

  def start(ready: Promise[Ready]): Future[Ready] = {
    logger.debug("source <{}> triggering ready statement <{}@{}>",
                 getName, ready, ready.hashCode.toHexString)
    ref ! Open(ready)
    ready.future
  }

  def stop(closed: Promise[Closed]): Future[Closed] = {
    logger.debug("source <{}> triggering closed statement <{}@{}>",
                 getName, closed, closed.hashCode.toHexString)
    ref ! Close(closed)
    closed.future
  }
}

/**
 * Atiesh source component with constructor.
 */
abstract class AtieshSource(name: String,
                            dispatcher: String,
                            cfg: Configuration,
                            interceptors: List[Interceptor],
                            sinks: List[Sink],
                            strategy: String)
  extends Source {
  final def getName: String = name
  final def getDispatcher: String = dispatcher
  final def getConfiguration: Configuration = cfg
  final def getExecutionContext: ExecutionContext = sec
  final def getInterceptors: List[Interceptor] = interceptors
  final def getSinks: List[Sink] = sinks
  final def getSinkSelectStrategy: String = strategy
}

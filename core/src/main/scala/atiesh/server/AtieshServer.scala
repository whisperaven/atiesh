/**
 * Copyright (C) Hao Feng
 */

package atiesh.server

// java
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
// scala
import scala.util.{ Try, Success, Failure }
import scala.concurrent.{ Promise, Await }
import scala.concurrent.duration._
// akka
import akka.actor.ActorSystem
// internal
import atiesh.component.{ Component, ExtendedComponent }
import atiesh.statement.{ Ready, Closed }
import atiesh.metrics.{ Metrics, ComponentMetrics }
import atiesh.source.Source
import atiesh.interceptor.Interceptor
import atiesh.sink.Sink
import atiesh.utils.{ Configuration, Extension, Logging,
                      ConfigParseException }

trait ServerType {
  final val componentType: String = "atiesh"
}

object AtieshServer extends ServerType with Logging {
  private[server] def componentConfiguration(cType: String): String =
    s"${componentType}.${cType}"

  private[server] def initializeComponents[C](
    cfg: Configuration,
    cType: String)(init: Configuration => Try[List[C]]): List[C] = {
    (cfg.getSection(componentConfiguration(cType)) match {
      case Some(c) =>
        init(c)
      case None =>
        throw new ConfigParseException(
          s"bad configurations, missing <${componentConfiguration(cType)}> " +
          s"section")
    }) match {
      case Success(comps) =>
        comps
      case Failure(exc) =>
        throw exc
    }
  }

  private[server] def startComponents(
    components: List[ExtendedComponent]): Unit = {
    components.map(c => {
      val p = Promise[Ready]()
      logger.info("atiesh server starting {} component <{}>",
                  c.componentType, c.getName)
      (c -> c.start(p))
    }).foreach({
      case (c, f) =>
        Await.ready(f, Duration.Inf).value.get match {
          case Success(_) =>
            logger.info("atiesh {} <{}> started", c.componentType, c.getName)
          case Failure(exc) =>
            throw exc
        }
    })
  }

  private[server] def stopComponents(
    components: List[ExtendedComponent]): Unit = {
    components.map(c => {
      val p = Promise[Closed]()
      logger.info("atiesh server stopping {} component <{}>",
                  c.componentType, c.getName)
      (c -> c.stop(p))
    }).foreach({
      case (c, f) =>
        Await.ready(f, Duration.Inf).value.get match {
          case Success(_) =>
            logger.info("atiesh {} <{}> stopped", c.componentType, c.getName)
          case Failure(exc) =>
            throw exc
        }
    })
  }
}

/**
 * Atiesh main server that assemble all components.
 */
class AtieshServer(cfg: Configuration)
  extends Component
  with ServerType
  with ComponentMetrics
  with Logging {
  import AtieshServer._

  private[this] val startingUp = new AtomicBoolean(true)
  private[this] val shuttingDown = new AtomicBoolean(false)
  private[this] val shutdownLatch = new CountDownLatch(1)

  private[this] var extensions: List[Extension] = _
  private[this] var sinks: List[Sink] = _
  private[this] var interceptors: List[Interceptor] = _
  private[this] var sources: List[Source] = _

  final private[this] implicit val system =
    ActorSystem("guardian", cfg.unwrapped)

  final def getName = componentType
  final def getConfiguration = cfg

  def awaitShutdown(): Unit = shutdownLatch.await()

  def assemble() {
    logger.info("assembling atiesh component")

    try {
      /**
       * Components initialize order:
       *  1st - metrics
       *  2nd - extensions
       *  3rd - interceptors
       *  4th - sinks
       *  5th - sources
       */
      Metrics.initializeMetrics(cfg)
      extensions =
        initializeComponents(cfg, Extension.componentType)(c => {
          Extension.initializeComponents(c)
        })

      interceptors =
        initializeComponents(cfg, Interceptor.componentType)(c => {
          Interceptor.initializeComponents(c)
        })

      sinks =
        initializeComponents(cfg, Sink.componentType)(c => {
          Sink.initializeComponents(c)
        })

      sources = initializeComponents(cfg, Source.componentType)(c => {
          Source.initializeComponents(c, interceptors, sinks)
        })

      logger.info("starting atiesh component")

      startComponents(extensions)
      startComponents(sinks)
      startComponents(sources)
    } catch {
      case exc: Throwable =>
        logger.error("fatal error during atiesh initialize & " +
                     "startup, prepare to shutdown", exc)
        startingUp.set(false)
        disassemble()
    }
    metricsComponentStartTimestampGauge.update(System.currentTimeMillis)
    startingUp.set(false)

    logger.info("atiesh server assembled")
  }

  def disassemble(): Unit = {
    if (startingUp.get) {
      throw new IllegalStateException("atiesh server is still starting up, " +
                                      "cannot shut down (probably killed " +
                                      "during startup)")
    }

    if (shutdownLatch.getCount > 0 &&
        shuttingDown.compareAndSet(false, true)) {
      logger.info("shutting down atiesh server")

      stopComponents(sources)
      stopComponents(sinks)
      stopComponents(extensions)

      Await.ready(system.terminate(), Duration.Inf).value.get match {
        case Success(r) =>
          logger.info("atiesh server shutdonw completed")
        case Failure(exc) =>
          logger.error("got unexpected exception during server " +
                       "shutting down, atiesh server force terminated", exc)
      }

      logger.info("stopping atiesh kamon metrics modules")
      Metrics.shutdownMetrics()

      shuttingDown.set(false)
      shutdownLatch.countDown()
    }
  }
}

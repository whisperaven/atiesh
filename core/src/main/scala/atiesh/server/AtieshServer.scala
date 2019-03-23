/*
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
// typesafe akka-actor
import akka.actor.ActorSystem
// internal
import atiesh.statement.Ready
import atiesh.source.Source
import atiesh.interceptor.Interceptor
import atiesh.sink.Sink
import atiesh.utils.{ Configuration, Component, ClassLoader, Logging }
import atiesh.metrics.Metrics

object AtieshServer extends Logging {
  val getComponentName: String = "atiesh"
  def getComponentName(componentName: String): String = s"${getComponentName}.${componentName}"

  def initializeComponent[C](cfg: Configuration)(initComp: Configuration => Try[C]): C =
    initializeComponent[C](cfg, None)(initComp)
  def initializeComponent[C](cfg: Configuration, componentName: String)(initComp: Configuration => Try[C]): C =
    initializeComponent[C](cfg, Some(getComponentName(componentName)))(initComp)

  def initializeComponent[C](cfg: Configuration, componentName: Option[String])(initComp: Configuration => Try[C]): C = {
    (cfg.getSection(componentName.getOrElse(getComponentName)) match {
      case Some(c) =>
        initComp(c)
      case None =>
        logger.error("bad configurations, missing <{}> section, initialize aborted", componentName)
        sys.exit(2)
    }) match {
      case Success(comps) =>
        comps
      case Failure(exc) =>
        logger.error("got unexpected error during server initialize, initialize aborted", exc)
        sys.exit(2)
    }
  }

  def apply(cfg: Configuration): AtieshServer = {
    new AtieshServer(cfg)
  }
}

class AtieshServer(cfg: Configuration) extends Logging {
  import AtieshServer._

  private val startingUp = new AtomicBoolean(true)
  private val shuttingDown = new AtomicBoolean(false)

  private var shutdownLatch = new CountDownLatch(1)
  private implicit val system = ActorSystem("atiesh_actorsystem", cfg.unwrapped)

  private var components: List[Component] = _
  private var sinks: List[Sink] = _
  private var sources: List[Source] = _
  private var interceptors: List[Interceptor] = _

  def startup() {
    logger.info("initialize atiesh server")

    /*
     * initialize metrics components first
     */
    initializeComponent(cfg, Metrics.getComponentName)(c => { Metrics.initializeMetrics(c)(cfg) })

    /*
     * initialize helper components before source & interceptor & sink
     *    other components may depends on these helper components like CachedProxy
     */
    components = initializeComponent(cfg)(c => { Component.initializeComponents(c) })

    interceptors = initializeComponent(cfg, Interceptor.getComponentName)(c => { Interceptor.initializeComponents(c) })
    sinks = initializeComponent(cfg, Sink.getComponentName)(c => { Sink.initializeComponents(c) })
    sources = initializeComponent(cfg, Source.getComponentName)(c => { Source.initializeComponents(c, interceptors, sinks) })

    sinks
      .map(s => {
        val p = Promise[Ready]()
        logger.info("atiesh server initialize components, starting sink <{}>", s.getName)
        (s -> s.open(p))
      }).foreach({ case (sink, readyFuture) =>
        Await.ready(readyFuture, Duration.Inf).value.get match {
          case Success(ready) =>
            logger.info("atiesh sink <{}> opened", ready.component)
          case Failure(exc) =>
            throw exc
        }
      })

    sources
      .map(s => {
        val p = Promise[Ready]()
        logger.info("atiesh server initialize components, starting source <{}>", s.getName)
        (s -> s.open(p))
      }).foreach({ case (source, readyFuture) =>
        Await.ready(readyFuture, Duration.Inf).value.get match {
          case Success(ready) =>
            logger.info("atiesh source <{}> opened", ready.component)
          case Failure(exc) =>
            throw exc
        }
      })

    logger.info("atiesh server started")

    shutdownLatch = new CountDownLatch(1)
    startingUp.set(false)
  }

  def awaitShutdown(): Unit = shutdownLatch.await()

  def shutdown(): Unit = {
    if (startingUp.get) {
      throw new IllegalStateException("atiesh server is still starting up, cannot shut down!")
    }

    if (shuttingDown.compareAndSet(false, true)) {
      logger.info("shutting down atiesh server")

      Source.shutdownComponents(sources)
      Sink.shutdownComponents(sinks)
      Component.shutdownComponents(components)

      Await.ready(system.terminate(), Duration.Inf).value.get match {
        case Success(r) =>
          logger.info("atiesh server shutdonw gracefully")
        case Failure(exc) =>
          logger.error("got unexpected exception during server shutting down, atiesh server force terminated", exc)
      }
      shutdownLatch.countDown()
    }
  }
}

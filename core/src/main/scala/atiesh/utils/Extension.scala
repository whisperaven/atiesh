/*
 * Copyright (C) Hao Feng
 */

package atiesh.utils

// scala
import scala.util.{ Try, Success, Failure }
import scala.concurrent.{ ExecutionContext, Promise, Future }
// akka
import akka.actor.ActorSystem
// internal
import atiesh.component.ExtendedComponent
import atiesh.metrics.ComponentMetrics
import atiesh.statement.{ Ready, Closed }

trait ExtensionType {
  final val componentType = "extension"
}

object Extension extends ExtensionType with Logging {
  object ExtensionOpts {
    val OPT_FQCN = "fqcn"
    val OPT_AKKA_DISPATCHER = "akka-dispatcher"
    val DEF_AKKA_DISPATCHER = "akka.actor.default-dispatcher"
  }

  private[atiesh] def initializeComponents(
    ecfs: Configuration)(implicit system: ActorSystem): Try[List[Extension]] =
    ecfs.getSections()
      /* read & parse configuration sections */
      .foldLeft(List[(String, Configuration)]())((cfgs, sn) => {
        ecfs.getSection(sn).map(ecf => {
          logger.debug("readed configuration section <{}> with content " +
                       "<{}> for create extension(s)", sn, ecf)
          (sn -> ecf) :: cfgs
        }).getOrElse(cfgs)
      })
      /* load & initialize */
      .foldLeft(List[Try[Extension]]())({
        case (comps, (name, cfg)) => initializeComponent(name, cfg) :: comps
      })
      /* inspect */
      .foldLeft(Try(List[Extension]()))((extensions, extension) => {
        extensions.flatMap(exts => {
          extension match {
            case Success(ext) => Success(ext :: exts)
            case Failure(exc) => Failure(exc)
          }
        })
      })

  private[utils] def initializeComponent(
    name: String,
    ecf: Configuration)(implicit system: ActorSystem): Try[Extension] = Try {
    val fqcn = ecf.getString(ExtensionOpts.OPT_FQCN)
    val dispatcher = ecf.getString(ExtensionOpts.OPT_AKKA_DISPATCHER,
                                   ExtensionOpts.DEF_AKKA_DISPATCHER)

    logger.debug("loading extension {} (class: {}) with configuration: {}",
                 name, fqcn, ecf)
    val extension = ComponentLoader.createInstanceFor[Extension](
      fqcn, List[(Class[_], AnyRef)](
        (classOf[String], name),
        (classOf[String], dispatcher),
        (classOf[Configuration], ecf)
      ))
    extension.bootstrap()

    logger.info("loaded extensions {} (class: {}) with configuration: {}",
                extension.getName, fqcn, extension.getConfiguration)
    extension
  }
}

/**
 * Atiesh extension component.
 */
trait Extension
  extends ExtendedComponent
  with ExtensionType
  with ComponentMetrics {
  @volatile final protected var sec: ExecutionContext = _

  def bootstrap()(implicit system: ActorSystem): Unit = {
    sec = system.dispatchers.lookup(getDispatcher)
  }

  def start(ready: Promise[Ready]): Future[Ready] = {
    try {
      startup()
      metricsComponentStartTimestampGauge.update(System.currentTimeMillis)
      ready.success(Ready(getName))
    } catch {
      case exc: Throwable => ready.failure(exc)
    }
    ready.future
  }

  def stop(closed: Promise[Closed]): Future[Closed] = {
    try {
      shutdown()
      closed.success(Closed(getName))
    } catch {
      case exc: Throwable => closed.failure(exc)
    }
    closed.future
  }
}

/**
 * Atiesh extension component with constructor.
 */
abstract class AtieshExtension(name: String,
                               dispatcher: String,
                               cfg: Configuration)
  extends Extension {
  final def getName: String = name
  final def getDispatcher: String = dispatcher
  final def getConfiguration: Configuration = cfg
  final def getExecutionContext: ExecutionContext = sec
}

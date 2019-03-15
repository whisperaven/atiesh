/*
 * Copyright (C) Hao Feng
 */

package atiesh.interceptor

// scala
import scala.util.{ Try, Success, Failure }
// internal
import atiesh.event.Event
import atiesh.utils.{ ClassLoader, Configuration, Logging }

trait Interceptor {
  def getName: String
  def getConfiguration: Configuration
  def getPriority: Int

  def intercept(event: Event): Event
}

abstract class AtieshInterceptor(name: String, priority: Int, cfg: Configuration) extends Interceptor {
  def getName: String = name
  def getConfiguration: Configuration = cfg
  def getPriority: Int = priority
}

object Interceptor extends Logging {
  object ComponentOpts {
    val OPT_INTERCEPTOR_CLSNAME = "type"
    val OPT_INTERCEPTOR_PRIORITY = "priority"
  }
  def getComponentName: String = "interceptors"

  def initializeComponents(icf: Configuration): Try[List[Interceptor]] = {
    val icfs = icf.getSections()
      .foldLeft(List[(String, Configuration)]())((cfgs, sn) => {
        icf.getSection(sn).map(ic => {
          logger.debug(
            "readed configuration section <{}> with content <{}> for create interceptor(s)", sn, ic)
          (sn -> ic) :: cfgs
        }).getOrElse(cfgs)
      })

    val comps = icfs.foldLeft(List[Try[Interceptor]]())({ case (comps, (name, cfg)) => initializeComponent(name, cfg) :: comps })

    comps.foldLeft(Try(List[Interceptor]()))((interceptors, interceptor) => {
      interceptors.flatMap(itps => {
        interceptor match {
          case Success(i) => Success(i :: itps)
          case Failure(exc) => Failure(exc)
        }
      })
    }).map(_.sortBy(-_.getPriority))
  }

  def initializeComponent(name: String, icf: Configuration): Try[Interceptor] = Try {
    val className = icf.getString(ComponentOpts.OPT_INTERCEPTOR_CLSNAME)
    val priority = icf.getInt(ComponentOpts.OPT_INTERCEPTOR_PRIORITY)

    logger.debug("loading interceptor {} (class: {}) with configuration: {}", name, className, icf)
    val interceptor = ClassLoader.loadClassInstanceByName[Interceptor](
      className, List[(AnyRef, Class[_])](
        (name, classOf[String]),
        (Int.box(priority), classOf[Int]),
        (icf, classOf[Configuration])
      ))
    logger.info("loaded interceptor {} (class: {}) with configuration: {}", interceptor.getName, className, interceptor.getConfiguration)

    interceptor
  }
}

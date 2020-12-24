/*
 * Copyright (C) Hao Feng
 */

package atiesh.interceptor

// scala
import scala.util.{ Try, Success, Failure }
// internal
import atiesh.event.Event
import atiesh.component.Component
import atiesh.utils.{ ComponentLoader, Configuration, Logging }

trait InterceptorType {
  final val componentType = "interceptor"
}

object Interceptor extends InterceptorType with Logging {
  object InterceptorOpts {
    val OPT_INTERCEPTOR_FQCN = "fqcn"
    val OPT_INTERCEPTOR_PRIORITY = "priority"
  }

  private[atiesh] def initializeComponents(
    icfs: Configuration): Try[List[Interceptor]] =
    icfs.getSections()
      /* read & parse configuration sections */
      .foldLeft(List[(String, Configuration)]())((cfgs, sn) => {
        icfs.getSection(sn).map(icf => {
          logger.debug("readed configuration section <{}> with content " +
                       "<{}> for create interceptor(s)", sn, icf)
          (sn -> icf) :: cfgs
        }).getOrElse(cfgs)
      })
      /* load & initialize */
      .foldLeft(List[Try[Interceptor]]())({
        case (comps, (name, cfg)) => initializeComponent(name, cfg) :: comps
      })
      /* inspect */
      .foldLeft(Try(List[Interceptor]()))((interceptors, interceptor) => {
        interceptors.flatMap(itps => {
          interceptor match {
            case Success(i) => Success(i :: itps)
            case Failure(exc) => Failure(exc)
          }
        })
      })
      /* sort by priority */
      .map(_.sortBy(-_.getPriority))

  private[interceptor] def initializeComponent(
    name: String,
    icf: Configuration): Try[Interceptor] = Try {
    val fqcn = icf.getString(InterceptorOpts.OPT_INTERCEPTOR_FQCN)
    val priority = icf.getInt(InterceptorOpts.OPT_INTERCEPTOR_PRIORITY)

    logger.debug("loading interceptor {} (class: {}) with configuration: {}",
                 name, fqcn, icf)
    val interceptor = ComponentLoader.createInstanceFor[Interceptor](
      fqcn, List[(Class[_], AnyRef)](
        (classOf[String], name),
        (classOf[Int], Int.box(priority)),
        (classOf[Configuration], icf)
      ))
    logger.info("loaded interceptor {} (class: {}) with configuration: {}",
                interceptor.getName, fqcn, interceptor.getConfiguration)

    interceptor
  }
}

/**
 * Atiesh interceptor component.
 */
trait Interceptor
  extends Component
  with InterceptorType
  with InterceptorMetrics {
  def getPriority: Int
  def intercept(event: Event): Event
}

/**
 * Atiesh interceptor component with constructor.
 */
abstract class AtieshInterceptor(name: String,
                                 priority: Int,
                                 cfg: Configuration)
  extends Interceptor {
  final def getName: String = name
  final def getConfiguration: Configuration = cfg

  def getPriority: Int = priority
}

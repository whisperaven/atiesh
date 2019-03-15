/*
 * Copyright (C) Hao Feng
 */

package atiesh.utils

// scala
import scala.util.{ Try, Success, Failure }
// akka
import akka.actor.ActorSystem
// internal
import atiesh.statement.{ Closed }

trait Component {
  def getComponentName: String
  def getConfiguration: Configuration

  def bootstrap()(implicit system: ActorSystem): Component
  def shutdown(): Unit
}

abstract class AtieshComponent(name: String, cfg: Configuration) extends Component {
  def getConfiguration: Configuration = cfg
  def getComponentName: String = name
}

object Component extends Logging {
  object ComponentOpts {
    val OPT_COMPONENT_CLSNAME = "component-class"
  }

  def initializeComponents(ccf: Configuration)(implicit system: ActorSystem): Try[List[Component]] = {
    val ccfs = ccf.getSections()
      .foldLeft(List[(String, Configuration)]())((cfgs, sn) => {
        ccf.getSection(sn).flatMap(cc => {
          cc.getStringOption(ComponentOpts.OPT_COMPONENT_CLSNAME)
            .map(_ => (sn -> cc) :: cfgs)
        }).getOrElse(cfgs)
      })
    
    val comps = ccfs.foldLeft(List[Try[Component]]())({ case (comps, (name, cfg)) => initializeComponent(name, cfg) :: comps })

    comps.foldLeft(Try(List[Component]()))((comps, comp) => {
      comps.flatMap(cps => {
        comp match {
          case Success(c) => Success(c :: cps)
          case Failure(exc) => Failure(exc)
        }
      })
    })
  }

  def initializeComponent(name: String, ccf: Configuration)(implicit system: ActorSystem): Try[Component] = Try {
    val className = ccf.getString(ComponentOpts.OPT_COMPONENT_CLSNAME)

    logger.debug("loading component {} (class: {}) with configuration: {}", name, className, ccf)
    val comp = ClassLoader.loadClassInstanceByName[Component](
      className, List[(AnyRef, Class[_])](
        (name, classOf[String]),
        (ccf, classOf[Configuration])
    )).bootstrap()
    logger.info("loaded component {} (class: {}) with configuration: {}", comp.getComponentName, className, comp.getConfiguration)

    comp
  }

  def shutdownComponents(comps: List[Component]): Unit = {
    comps.foreach(comp => {
      logger.info("shutting donw atiesh component, shutting down <{}>", comp.getComponentName)
      comp.shutdown()
      logger.info("atiesh component <{}> shutdown", comp.getComponentName)
    })
  }
}

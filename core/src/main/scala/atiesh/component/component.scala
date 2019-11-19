/*
 * Copyright (C) Hao Feng
 */

package atiesh.component

// scala
import scala.concurrent.{ ExecutionContext, Promise, Future }
// akka
import akka.actor.ActorSystem
// internal
import atiesh.utils.Configuration
import atiesh.statement.{ Ready, Closed }

/**
 * Atiesh simplest component interface.
 */
trait Component {
  val componentType: String

  def getName: String
  def getConfiguration: Configuration
}

/**
 * Atiesh extended component interface.
 */
trait ExtendedComponent extends Component {
  def getDispatcher: String
  def getExecutionContext: ExecutionContext

  def bootstrap()(implicit system: ActorSystem): Unit

  def start(ready: Promise[Ready]): Future[Ready]
  def startup(): Unit

  def stop(closed: Promise[Closed]): Future[Closed]
  def shutdown(): Unit
}

/*
 * Copyright (C) Hao Feng
 */

package atiesh.source

// internal
import atiesh.event.{ Event, Empty, SimpleEvent }
import atiesh.interceptor.Interceptor
import atiesh.sink.Sink
import atiesh.utils.{ Configuration, Logging }
import atiesh.metrics.MetricsGroup._

object DevZero {
  object DevZeroOpts {
    val OPT_BATCH_SIZE = "batch-size"
    val DEF_BATCH_SIZE = 1024L
  } 
}

class DevZero(
  name: String,
  dispatcher: String,
  cfg: Configuration,
  interceptors: List[Interceptor],
  sinks: List[Sink],
  strategy: String) extends AtieshSource(name, dispatcher, cfg, interceptors, sinks, strategy) with Logging {
  import DevZero._

  val batchSize = cfg.getLong(DevZeroOpts.OPT_BATCH_SIZE, DevZeroOpts.DEF_BATCH_SIZE)

  def mainCycle(): Unit = {
    (0L to batchSize).foreach(_ => {
      val event = SimpleEvent("0", Map[String, String]())

      intercept(event) match {
        case Empty =>
          logger.debug("event <{}> discarded by interceptor", event.getBody)
        case intercepted: Event =>
          sink(intercepted)
      }
    })
    commit()
  }

  def startup(): Unit = {
    logger.debug("source <{}> initialize devzero source instances", getName)
  }

  def shutdown(): Unit = {
    logger.debug("closing source <{}>", getName)
  }
}

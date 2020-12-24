/*
 * Copyright (C) Hao Feng
 */

package atiesh.source

// internal
import atiesh.event.{ Event, Empty, SimpleEvent }
import atiesh.interceptor.Interceptor
import atiesh.sink.Sink
import atiesh.utils.{ Configuration, Logging }

object DevZero {
  object DevZeroOpts {
    val OPT_BATCH_SIZE = "batch-size"
    val DEF_BATCH_SIZE = 1024L
  }
}

/**
 * Atiesh builtin source component, produce zero continuous.
 */
class DevZero(
  name: String,
  dispatcher: String,
  cfg: Configuration,
  interceptors: List[Interceptor],
  sinks: List[Sink],
  strategy: String)
  extends AtieshSource(name, dispatcher, cfg, interceptors, sinks, strategy)
  with ActiveSourceSemantics
  with Logging {
  import DevZero.{ DevZeroOpts => Opts }
  import Source.Acknowledge

  val batchSize = cfg.getLong(Opts.OPT_BATCH_SIZE, Opts.DEF_BATCH_SIZE)

  def mainCycle(): List[Event] =
    (0L to batchSize).foldLeft(List[Event]())((es, z) => {
      SimpleEvent("0", Map[String, String]()) :: es
    })

  def startup(): Unit = {
    logger.debug("source <{}> initialize devzero source instances", getName)
  }

  def shutdown(): Unit = {
    logger.debug("closing source <{}>", getName)
  }
}

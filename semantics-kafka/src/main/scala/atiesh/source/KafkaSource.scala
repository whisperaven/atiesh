/*
 * Copyright (C) Hao Feng
 */

package atiesh.source

// internal
import atiesh.interceptor.Interceptor
import atiesh.sink.Sink
import atiesh.utils.{ Configuration, Logging }

class KafkaSource(name: String,
                  dispatcher: String,
                  cfg: Configuration,
                  interceptors: List[Interceptor],
                  sinks: List[Sink],
                  strategy: String)
  extends AtieshSource(name, dispatcher, cfg, interceptors, sinks, strategy)
  with ActiveSourceSemantics
  with KafkaSourceSemantics
  with Logging {

  def shutdown(): Unit = {
    logger.info("shutting down kafka source <{}>", getName)
  }

  def startup(): Unit = {
    logger.info("starting source kafka <{}>", getName)
  }
}

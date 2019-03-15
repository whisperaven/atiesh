/*
 * Copyright (C) Hao Feng
 */

package atiesh.source

// scala
import atiesh.interceptor.Interceptor
import atiesh.sink.Sink
import atiesh.utils.{ Configuration, Logging }

class KafkaSource(
  name: String,
  dispatcher: String,
  cfg: Configuration,
  interceptors: List[Interceptor],
  sinks: List[Sink],
  strategy: String)
  extends AtieshSource(name, dispatcher, cfg, interceptors, sinks, strategy)
  with KafkaSourceSemantics
  with Logging {

  def shutdown(): Unit = { 
    logger.info("shutting down source <{}>", getName)
  }

  def startup(): Unit = { 
    logger.info("starting source <{}>", getName)
  }

}

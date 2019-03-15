/*
 * Copyright (C) Hao Feng
 */

package atiesh.utils

import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.Logger

/* same as the StrictLogging of scala-logging */
trait Logging {
  protected val logger: Logger = Logger(LoggerFactory.getLogger(getClass.getName))
}

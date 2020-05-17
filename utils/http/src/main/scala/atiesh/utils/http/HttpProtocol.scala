/*
 * Copyright (C) Hao Feng
 */

package atiesh.utils.http

// akka
import akka.http.scaladsl.model.{ HttpProtocol => AkkaHttpProtocol,
                                  HttpProtocols => AkkaHttpProtocols }

/**
 * Standard http protocol type and constants.
 */
object HttpProtocols {
  type HttpProtocol = AkkaHttpProtocol
  val HTTP_PROTOCOL_10 = AkkaHttpProtocols.`HTTP/1.0`
  val HTTP_PROTOCOL_11 = AkkaHttpProtocols.`HTTP/1.1`
  val HTTP_PROTOCOL_20 = AkkaHttpProtocols.`HTTP/2.0`
}
import HttpProtocols.HttpProtocol

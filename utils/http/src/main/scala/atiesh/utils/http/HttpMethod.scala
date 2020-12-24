/*
 * Copyright (C) Hao Feng
 */

package atiesh.utils.http

// akka
import akka.http.scaladsl.model.{ HttpMethod => AkkaHttpMethod,
                                  HttpMethods => AkkaHttpMethods }

/**
 * Standard http method type and constants.
 */
object HttpMethods {
  type HttpMethod = AkkaHttpMethod
  val GET         = AkkaHttpMethods.GET
  val POST        = AkkaHttpMethods.POST
  val PUT         = AkkaHttpMethods.PUT
  val DELETE      = AkkaHttpMethods.DELETE
  val HEAD        = AkkaHttpMethods.HEAD
  val CONNECT     = AkkaHttpMethods.CONNECT
  val OPTIONS     = AkkaHttpMethods.OPTIONS
  val PATCH       = AkkaHttpMethods.PATCH
  val TRACE       = AkkaHttpMethods.TRACE
}
import HttpMethods.HttpMethod

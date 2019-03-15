package atiesh.utils.http

// akka
import akka.http.scaladsl.model.{ HttpResponse => AkkaHttpResponse, _ }

case class HttpResponse(status: Int, headers: Map[String, String], body: String)

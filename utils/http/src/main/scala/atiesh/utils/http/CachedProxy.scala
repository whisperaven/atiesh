/*
 * Copyright (C) Hao Feng
 */

package atiesh.utils.http

// java
import java.util.concurrent.{ ConcurrentHashMap => JCHashMap }
// scala
import scala.util.{ Try, Success, Failure }
import scala.concurrent.{ ExecutionContext, Await, Future }
import scala.concurrent.duration._
import scala.collection.JavaConverters._
// akka
import akka.actor.{ ActorSystem, Scheduler, Cancellable }
import akka.stream.{ ActorMaterializer, Materializer }
import akka.http.scaladsl.{ Http, HttpExt }
import akka.http.scaladsl.model.{
  ContentType => AkkaContentType,
  ContentTypes => AkkaContentTypes,
  HttpMethod => AkkaHttpMethod,
  HttpMethods => AkkaHttpMethods,
  HttpRequest => AkkaHttpRequest,
  HttpResponse => AkkaHttpResponse, _ }
import akka.util.ByteString
// internal
import atiesh.utils.{ Configuration, Logging, AtieshComponent, Component, UninitializedComponentException }

object CachedProxy {
  object CachedProxyOpts {
    // opt
    val OPT_CACHE_SIZE = "cache-size"
    val DEF_CACHE_SIZE = 16  /* the default initial table size of java.util.concurrent.ConcurrentHashMap */
    val OPT_MAX_RESPONSE_BODY_SIZE = "max-response-body-size"
    val DEF_MAX_RESPONSE_BODY_SIZE = 10485760 /* 10 mb */
  }

  var proxyInstance: Option[CachedProxy] = None
  def getProxyInstance: CachedProxy = proxyInstance match {
    case Some(instance) =>
      instance
    case _ =>
      throw new UninitializedComponentException("cached proxy not initialized, make sure you have <cached-proxy> config section")
  }
}
case class RegisteredRequest[T](req: HttpRequest, expire: FiniteDuration, cacheKey: String, formatter: String => T)

class CachedProxy(name: String, cpfg: Configuration) extends AtieshComponent(name, cpfg) with Logging {
  import CachedProxy._

  implicit var ec: ExecutionContext = _
  implicit var materializer: Materializer = _

  var http: HttpExt = _
  var scheduler: Scheduler = _

  var tasks: JCHashMap[String, (FiniteDuration, Cancellable)] = _
  var cache: JCHashMap[String, AnyRef] = _
  val nothing = new AnyRef

  var cacheSize: Int = _
  var maxResponseBodySize: Long = _

  def bootstrap()(implicit system: ActorSystem): Component = {
    val cfg = getConfiguration
    cacheSize = cfg.getInt(
      CachedProxyOpts.OPT_CACHE_SIZE,
      CachedProxyOpts.DEF_CACHE_SIZE)
    maxResponseBodySize = cfg.getBytes(
      CachedProxyOpts.OPT_MAX_RESPONSE_BODY_SIZE,
      CachedProxyOpts.DEF_MAX_RESPONSE_BODY_SIZE)

    http = Http()
    ec = system.dispatcher
    materializer = ActorMaterializer()
    scheduler = system.scheduler

    tasks = new JCHashMap[String, (FiniteDuration, Cancellable)](cacheSize)
    cache = new JCHashMap[String, AnyRef](cacheSize)

    proxyInstance = Some(this.asInstanceOf[CachedProxy])
    this
  }

  def registerRequest[T](
    req: HttpRequest,
    expire: FiniteDuration,
    cacheKeyGen: HttpRequest => String)(formatter: String => T):
    RegisteredRequest[T] = {
    val cacheKey = cacheKeyGen(req)
    if (!tasks.containsKey(cacheKey)) {
      val task = scheduler.schedule(expire, expire)({
        Await.ready(updateCache(req, cacheKey)(formatter), Duration.Inf).value.get match {
          case Success(cachedValue) =>
            logger.debug("scheduled cache update successful, got value {}", cachedValue)
          case Failure(exc) =>
            logger.error("scheduled cache update failed, continue using expired cache", exc)
        }
      })
      tasks.put(cacheKey, (expire, task))
      logger.debug(
        "cache update task create successful with expire <{}> and cacheKey <{}>, " +
        "total cache slot(s): <{}>",
        expire,
        cacheKey,
        tasks.size)
    } else {
      val (e, t) = tasks.get(cacheKey)
      if (e > expire) {
        tasks.put(cacheKey, (expire, t))
        logger.debug(
          "cache update task update successed with lower expire <{}> and cacheKey <{}>, " +
          "total cache slot(s): <{}>",
          expire,
          cacheKey,
          tasks.size)
      }
    }
    RegisteredRequest(req, expire, cacheKey, formatter)
  }

  val responseErrorHandler: PartialFunction[Throwable, ByteString] = {
    case _: EntityStreamSizeException =>
      throw new HttpResponseTooLargeException(
        s"response content size exceeded max response size limit (${maxResponseBodySize} bytes)," +
        s"you can configure this by setting <${CachedProxy.CachedProxyOpts.OPT_MAX_RESPONSE_BODY_SIZE}> " +
        s"in <${getComponentName}> section")
  }

  def updateCache[T](req: HttpRequest, cacheKey: String)(formatter: String => T): Future[T] = {
    val request = AkkaHttpRequest(req.akkaHttpMethod, req.akkaUri, req.akkaHttpHeaders, req.akkaHttpEntity)
    logger.debug("updating cache - creating request for <{}> to remote <{}>", cacheKey, req.uri)
    http.singleRequest(request).flatMap({
      case AkkaHttpResponse(StatusCodes.OK, _, entity, _) =>
        entity.withSizeLimit(maxResponseBodySize).dataBytes.recover(responseErrorHandler).runFold(ByteString(""))(_ ++ _)(materializer)
          .map(bs => {
            logger.debug("updating cache - formatting api data with user formatter -> <{}>", bs.utf8String)
            val content = formatter(bs.utf8String)
            logger.debug("cache updated with key -> {}, req -> {}, value -> {}", cacheKey, req, content)
            cache.put(cacheKey, content.asInstanceOf[AnyRef])
            content
          })
      case AkkaHttpResponse(status, _, entity, _) =>
        entity.withSizeLimit(maxResponseBodySize).dataBytes.recover(responseErrorHandler).runFold(ByteString(""))(_ ++ _)(materializer)
          .map(bs => {
            throw new UnexceptedHttpResponseException(s"got unexpected http response ${status} with message ${bs.utf8String}")
          })
    })
  }

  def validateCache[T](register: RegisteredRequest[T]): Try[T] = {
    val data = cache.getOrDefault(register.cacheKey, nothing)
    if (data == nothing) {
      logger.debug("cache validate {} - missing, pulling", register.req.uri)
      Await.ready(updateCache(register.req, register.cacheKey)(register.formatter), Duration.Inf).value.get
    } else {
      logger.debug("cache validate {} - hit, returning", register.req.uri)
      Success(data.asInstanceOf[T])
    }
  }

  def getCache[T](register: RegisteredRequest[T]): Option[T] = {
    val data = cache.getOrDefault(register.cacheKey, nothing)
    if (data == nothing) {
      logger.debug("cache access with registered cacheKey <{}> return nothing", register.cacheKey)
      None
    } else {
      logger.debug("cache access with registered cacheKey <{}> return cached data", register.cacheKey)
      Some(data.asInstanceOf[T])
    }
  }

  def shutdown(): Unit = {
    tasks.entrySet.asScala.foldLeft(())((r, e) => {
      val (expire, task) = e.getValue()
      if (task.cancel()) {
        logger.debug("canceled proxy task with cacheKey <{}>", e.getKey)
      } else {
        logger.warn("failed to cancel proxy task with cacheKey <{}>", e.getKey)
      }
    })
  }
}

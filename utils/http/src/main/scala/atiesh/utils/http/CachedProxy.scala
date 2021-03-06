/*
 * Copyright (C) Hao Feng
 */

package atiesh.utils.http

// java
import java.util.concurrent.{ ConcurrentHashMap => JCHashMap }
import java.util.function.{ BiFunction => JBiFunc }
// scala
import scala.util.{ Try, Success, Failure }
import scala.concurrent.{ ExecutionContext, Await, Promise, Future }
import scala.concurrent.duration._
import scala.collection.JavaConverters._
// akka
import akka.actor.{ ActorSystem, Scheduler, Cancellable }
import akka.stream.{ ActorMaterializer, Materializer }
import akka.http.scaladsl.{ Http, HttpExt }
import akka.http.scaladsl.model.{ HttpResponse => AkkaHttpResponse,
                                  StatusCodes => AkkaStatusCodes,
                                  EntityStreamSizeException }
import akka.util.ByteString
// internal
import atiesh.utils.{ Configuration, Logging, AtieshExtension,
                      UninitializedExtensionException }

object CachedProxy {
  object CachedProxyOpts {
    val OPT_CACHE_SIZE = "cache-size"
    val DEF_CACHE_SIZE = 16  /* default initial table size of JCHashMap */
    val OPT_ZERO_INITIAL_DELAY = "zero-initial-delay"
    val DEF_ZERO_INITIAL_DELAY = false
    val OPT_REQUEST_WITHOUT_SIZE_LIMITE = "request-without-size-limit"
    val DEF_REQUEST_WITHOUT_SIZE_LIMITE = true
    val OPT_RESPONSE_WITHOUT_SIZE_LIMITE = "response-without-size-limit"
    val DEF_RESPONSE_WITHOUT_SIZE_LIMITE = true
  }
  val NOTHING = new AnyRef
  final case class CacheUpdateTask(cacheExpire: FiniteDuration,
                                   updateTask: Cancellable) {
    def cancel(): Boolean = updateTask.cancel()
  }
  final case class RegisteredRequest[T](req: HttpRequest,
                                        expire: FiniteDuration,
                                        cacheKey: String,
                                        formatter: Array[Byte] => T)


  @volatile var proxyInstance: Option[CachedProxy] = None
  def getProxyInstance: CachedProxy = proxyInstance match {
    case Some(instance) =>
      instance
    case _ =>
      throw new UninitializedExtensionException(
        "http cached proxy not initialized, make sure you have fqcn " +
        "<atiesh.utils.http.CachedProxy> inside your <extension> " +
        "config section")
  }
}

/**
 * Atiesh http cached proxy class,
 * an util for access and cache external http resource.
 */
class CachedProxy(name: String, dispatcher: String, hcpcf: Configuration)
  extends AtieshExtension(name, dispatcher, hcpcf)
  with Logging {
  import CachedProxy.{ CachedProxyOpts => Opts, _ }

  final private[this] implicit var proxyExecutionContext: ExecutionContext = _
  final private[this] implicit var proxyMaterializer: Materializer = _

  final private[this] var proxyHttpContext: HttpExt = _
  final private[this] var proxyScheduler: Scheduler = _

  final private[this] var proxyCache: JCHashMap[String, AnyRef] = _
  final private[this] var proxyTasks: JCHashMap[String, CacheUpdateTask] = _
  final private[this] var proxySlots: JCHashMap[String, Promise[AnyRef]] = _

  final private[this] var proxyWithoutRequestSizeLimit: Boolean = _
  final private[this] var proxyWithoutResponseSizeLimit: Boolean = _

  final private[this] var proxyZeroInitialDelay: Boolean = _

  override def bootstrap()(implicit system: ActorSystem): Unit = {
    super.bootstrap()

    val cfg = getConfiguration
    val cacheSize = cfg.getInt(Opts.OPT_CACHE_SIZE, Opts.DEF_CACHE_SIZE)
    proxyZeroInitialDelay = cfg.getBoolean(Opts.OPT_ZERO_INITIAL_DELAY,
                                      Opts.DEF_ZERO_INITIAL_DELAY)
    proxyWithoutRequestSizeLimit =
      cfg.getBoolean(Opts.OPT_REQUEST_WITHOUT_SIZE_LIMITE,
                     Opts.DEF_REQUEST_WITHOUT_SIZE_LIMITE)
    proxyWithoutResponseSizeLimit =
      cfg.getBoolean(Opts.OPT_RESPONSE_WITHOUT_SIZE_LIMITE,
                     Opts.DEF_RESPONSE_WITHOUT_SIZE_LIMITE)

    proxyExecutionContext = getExecutionContext
    proxyMaterializer = ActorMaterializer()

    proxyHttpContext = Http()
    proxyScheduler = system.scheduler

    proxyTasks = new JCHashMap[String, CacheUpdateTask](cacheSize)
    proxyCache = new JCHashMap[String, AnyRef](cacheSize)
    proxySlots = new JCHashMap[String, Promise[AnyRef]](cacheSize)

    proxyInstance = Some(this)
  }

  def createScheduleTaskFor[T](
    req: HttpRequest,
    expire: FiniteDuration,
    cacheKey: String,
    formatter: Array[Byte] => T): CacheUpdateTask = {
    val initialDelay = if (proxyZeroInitialDelay) Duration.Zero else expire
    CacheUpdateTask(expire, proxyScheduler.schedule(initialDelay, expire)({
      Await.ready(updateCache(req, cacheKey)(formatter), Duration.Inf)
           .value.get match {
        case Success(cachedValue) =>
          logger.debug("scheduled cache update successful, " + 
                       "got value <{}>", cachedValue)
        case Failure(exc) =>
          logger.error("scheduled cache update failed, " +
                       "continue using expired cache", exc)
      }
    }))
  }

  def registerRequest[T](
    req: HttpRequest,
    expire: FiniteDuration,
    cacheKeyGen: HttpRequest => String)(
    formatter: Array[Byte] => T): RegisteredRequest[T] = {
    val cacheKey = cacheKeyGen(req)
    val injector = new JBiFunc[String, CacheUpdateTask, CacheUpdateTask] {
      override def apply(key: String,
                         value: CacheUpdateTask): CacheUpdateTask = {
        if (value == null) {
          createScheduleTaskFor(req, expire, cacheKey, formatter)
        } else {
          if (value.cacheExpire > expire) {
            logger.info("replace cache update task for cacheKey <{}> " +
                        "with lower expire <{}>, current total cache " +
                        "slot(s): <{}>", cacheKey, expire, proxyTasks.size)
            if (!value.cancel()) logger.warn(
              "failed to cancel previous registered task " +
              "for cachekey <{}>", cacheKey)
            createScheduleTaskFor(req, expire, cacheKey, formatter)
          } else {
            logger.debug("ignore cache update register with longer or " +
                         "equality cache expire <{}> and same " +
                         "cacheKey <{}>", expire, cacheKey)
            value
          }
        }
      }
    }
    RegisteredRequest(req,
                      proxyTasks.compute(cacheKey, injector).cacheExpire,
                      cacheKey,
                      formatter)
  }

  def updateCache[T](
    req: HttpRequest,
    cacheKey: String)(formatter: Array[Byte] => T): Future[T] = {
    val injector = new JBiFunc[String, Promise[AnyRef], Promise[AnyRef]] {
      override def apply(key: String,
                         value: Promise[AnyRef]): Promise[AnyRef] = {
        if (value == null || value.isCompleted) {
          val p = Promise[AnyRef]()
          logger.debug("updating cache - creating request for <{}>, to " +
                       "remote endpoint <{}> with query <{}> and body <{}>",
                       cacheKey, req.uri, req.queries, req.stringBody)

          proxyHttpContext.singleRequest(
            req.akkaHttpRequest.mapEntity(entity => {
              if (proxyWithoutRequestSizeLimit) {
                entity.withoutSizeLimit()
              } else entity
            })).flatMap({
            case AkkaHttpResponse(AkkaStatusCodes.OK, _, entity, _) =>
              val bodyStream = {
                if (proxyWithoutResponseSizeLimit) {
                  entity.withoutSizeLimit().dataBytes
                } else entity.dataBytes
              }
              bodyStream.runFold(ByteString.empty)(_ ++ _)(proxyMaterializer)
                .map(bs => {
                  logger.debug("now updating cache for cacheKey <{}>, " +
                               "formatting api data with given formatter",
                               cacheKey)
                  val content = formatter(bs.toArray)
                  proxyCache.put(cacheKey, content.asInstanceOf[AnyRef])
                  logger.debug("cache updated, key -> {}, value -> {}",
                               cacheKey, content)
                  proxyCache.get(cacheKey)
                })(proxyExecutionContext)
            case AkkaHttpResponse(status, _, entity, _) =>
              val bodyStream = {
                if (proxyWithoutResponseSizeLimit) {
                  entity.withoutSizeLimit().dataBytes
                } else entity.dataBytes
              }
              bodyStream.runFold(ByteString.empty)(_ ++ _)(proxyMaterializer)
                .map(bs => {
                  throw new UnexceptedHttpResponseException(
                    s"got unexpected http response ${status} with " +
                    s"message ${bs.utf8String} (UTF_8)")
                })(proxyExecutionContext)
          /* make sure the promise completed */
          })(proxyExecutionContext).onComplete({
            case Failure(exc) =>
              p.failure(exc)
            case Success(cacheContent) =>
              p.success(cacheContent)
              logger.debug(
                "cache update request for <{}> complete successful", cacheKey)
          })(proxyExecutionContext)
          p
        } else {
          value
        }
      }
    }
    proxySlots.compute(cacheKey, injector)
              .future.map(_.asInstanceOf[T])(proxyExecutionContext)
  }

  def validateCache[T](register: RegisteredRequest[T]): Try[T] = {
    val data = proxyCache.getOrDefault(register.cacheKey, NOTHING)
    if (data == NOTHING) {
      logger.debug("cache validate for key <{}> - missing, pulling",
                   register.cacheKey)
      Await.ready(updateCache(register.req,
                              register.cacheKey)(register.formatter),
                  Duration.Inf).value.get
    } else {
      logger.debug("cache validate for key <{}> - hit, returning",
                   register.cacheKey)
      Success(data.asInstanceOf[T])
    }
  }

  def getCache[T](register: RegisteredRequest[T]): Option[T] = {
    val data = proxyCache.getOrDefault(register.cacheKey, NOTHING)
    if (data == NOTHING) {
      logger.debug("cache access with registered cacheKey <{}> " +
                   "return nothing", register.cacheKey)
      None
    } else {
      logger.debug("cache access with registered cacheKey <{}> " +
                   "return cached data", register.cacheKey)
      Some(data.asInstanceOf[T])
    }
  }

  def startup(): Unit = {
    logger.info("starting http cached proxy <{}>", getName)
  }

  def shutdown(): Unit = proxyTasks.entrySet.asScala.foldLeft(())((r, e) => {
    if (e.getValue().cancel())
      logger.debug("canceled proxy task for cacheKey <{}>", e.getKey)
    else
      logger.warn("failed to cancel proxy task for cacheKey <{}>", e.getKey)
  })
}

/*
 * Copyright (C) Hao Feng
 */

package atiesh.sink

// scala
import scala.util.{ Try, Success, Failure }
import scala.concurrent.{ ExecutionContext, Promise, Future }
import scala.collection.JavaConverters._
// akka
import akka.actor.ActorSystem
// aliyun
import com.aliyun.openservices.log.common.LogItem
import com.aliyun.openservices.aliyun.log.producer.{ ProjectConfig,
                                                     ProjectConfigs,
                                                     ProducerConfig }
import com.aliyun.openservices.aliyun.log.producer.{ LogProducer,
                                                     Result, Callback }
// internal
import atiesh.event.Event
import atiesh.statement.{ Ready, Closed }
import atiesh.utils.{ Configuration, Logging }

object AliyunSLSSinkSemantics extends Logging {
  object AliyunSLSSinkSemanticsOpts {
    val OPT_AKKA_DISPATCHER = "sls-akka-dispatcher"
    val DEF_AKKA_DISPATCHER = "akka.actor.default-dispatcher"

    val OPT_SLS_PRODUCER_ACCESS_KEY_ID = "sls-access-key-id"
    val OPT_SLS_PRODUCER_ACCESS_KEY_SECRET = "sls-access-key-secret"
    val OPT_SLS_PRODUCER_ENDPOINT = "sls-endpoint"
    val OPT_SLS_PRODUCER_PROJEST_NAME = "sls-project-name"
    val OPT_SLS_PRODUCER_LOGSTORE_NAME = "sls-logstore-name"
  }

  type RecordParser  = Event => Try[ProduceRecord]
  type RecordsParser = List[Event] => Try[ProduceRecords]

  final case class ProduceRecord(project: String,
                                 logstore: String,
                                 item: LogItem,
                                 topic: Option[String],
                                 source: Option[String],
                                 shardHash: Option[String])
  object ProduceRecord {
    def apply(project: String,
              logstore: String,
              item: LogItem): ProduceRecord = {
      ProduceRecord(project, logstore, item, None, None, None)
    }
  }

  final case class ProduceRecords(project: String,
                                  logstore: String,
                                  items: List[LogItem],
                                  topic: Option[String],
                                  source: Option[String],
                                  shardHash: Option[String])
  object ProduceRecords {
    def apply(project: String,
              logstore: String,
              items: List[LogItem]): ProduceRecords = {
      ProduceRecords(project, logstore, items, None, None, None)
    }
  }

  def createProducer(owner: String, pcf: Configuration): LogProducer = {
    import AliyunSLSSinkSemanticsOpts._
    val endpoint = pcf.getString(OPT_SLS_PRODUCER_ENDPOINT)
    val accessKeyId = pcf.getString(OPT_SLS_PRODUCER_ACCESS_KEY_ID)
    val accessKeySecret = pcf.getString(OPT_SLS_PRODUCER_ACCESS_KEY_SECRET)
    val projectName = pcf.getString(OPT_SLS_PRODUCER_PROJEST_NAME)
    val logstoreName = pcf.getString(OPT_SLS_PRODUCER_LOGSTORE_NAME)

    val pjcf = new ProjectConfigs()
    pjcf.put(new ProjectConfig(projectName,
                               endpoint,
                               accessKeyId,
                               accessKeySecret))

    val producer = new LogProducer(new ProducerConfig(pjcf))
    logger.debug("sink <{}> producer created", owner)

    producer
  }
}

trait AliyunSLSSinkSemantics
  extends SinkSemantics
  with AliyunSLSSinkMetrics
  with Logging { this: Sink =>
  import AliyunSLSSinkSemantics.{ AliyunSLSSinkSemanticsOpts => Opts, _ }

  @volatile final private var aliyunSLSProducer: LogProducer = _

  @volatile final private var aliyunSLSDispatcher: String = _
  @volatile final private var aliyunSLSExecutionContext: ExecutionContext = _

  final def getAliyunSLSDispatcher: String =
    aliyunSLSDispatcher
  final def getAliyunSLSExecutionContext: ExecutionContext =
    aliyunSLSExecutionContext

  override def bootstrap()(implicit system: ActorSystem): Unit = {
    super.bootstrap()

    aliyunSLSDispatcher = getConfiguration.getString(Opts.OPT_AKKA_DISPATCHER,
                                                     Opts.DEF_AKKA_DISPATCHER)
    aliyunSLSExecutionContext = system.dispatchers.lookup(aliyunSLSDispatcher)
  }

  final def slsCreateProduceCB(p: Promise[Result], length: Int): Callback =
    new Callback {
      override def onCompletion(result: Result) {
        if (result.isSuccessful) {
          metricsSLSSinkEventPublishSuccessCounter.increment(length)
          metricsSLSSinkComponentEventPublishSuccessCounter.increment(length)
          p.success(result)
        } else {
          metricsSLSSinkEventPublishFailureCounter.increment(length)
          metricsSLSSinkComponentEventPublishFailureCounter.increment(length)
          p.failure(new AliyunSLSSinkException(
                      result.getReservedAttempts.asScala.toList))
        }
      }
    }
  final def slsCreateProduceCB(p: Promise[Result]): Callback =
    slsCreateProduceCB(p, 1)

  final def slsSend(event: Event, parser: RecordParser): Future[Result] = {
    val p = Promise[Result]
    val cb = slsCreateProduceCB(p)

    val result = parser(event) match {
      case Success(record) =>
        record match {
          case ProduceRecord(proj, store, item, None, None, None) => Try {
              aliyunSLSProducer.send(proj, store, item, cb)
            }
          case ProduceRecord(proj, store, item,
                             Some(topic), Some(source), None) => Try {
              aliyunSLSProducer.send(proj, store, topic, source, item, cb)
            }
          case ProduceRecord(proj, store, item,
                             Some(topic), Some(source), Some(hash)) => Try {
              aliyunSLSProducer.send(proj, store, topic, source, hash, item, cb)
            }
          case _ =>
            Failure(new SinkInvalidEventException(
              "bad event parser response, cannot invoke send, the parser " +
              "should return a ProduceRecord instance which contains " +
              "<project, logstore, item> or " +
              "<project, logstore, item, topic source> or " +
              "<project, logstore, item, topic, source, shardhash>"))
        }
      case failure @ Failure(_) => failure
    }

    result match {
      case Success(_)   => p.future
      case Failure(exc) => Future.failed[Result](exc)
    }
  }

  final def slsSend(events: List[Event],
                    parser: RecordsParser): Future[Result] = {
    val p = Promise[Result]
    val cb = slsCreateProduceCB(p)

    val result = parser(events) match {
      case Success(record) =>
        record match {
          case ProduceRecords(proj, store, items, None, None, None) => Try {
              aliyunSLSProducer.send(proj, store, items.asJava, cb)
            }
          case ProduceRecords(proj, store, items,
                              Some(topic), Some(source), None) => Try {
              aliyunSLSProducer.send(proj, store, topic,
                                     source, items.asJava, cb)
            }
          case ProduceRecords(proj, store, items,
                              Some(topic), Some(source), Some(hash)) => Try {
              aliyunSLSProducer.send(proj, store, topic,
                                     source, hash, items.asJava, cb)
            }
          case _ =>
            Failure(new SinkInvalidEventException(
              "bad event parser response, cannot invoke send, the parser " +
              "should return a ProduceRecords instance which contains " +
              "<project, logstore, items> or " +
              "<project, logstore, items, topic source> or " +
              "<project, logstore, items, topic, source, shardhash>"))
        }
      case failure @ Failure(_) => failure
    }

    result match {
      case Success(_)   => p.future
      case Failure(exc) => Future.failed[Result](exc)
    }
  }

  override def open(ready: Promise[Ready]): Unit = {
    aliyunSLSProducer = createProducer(getName, getConfiguration)
    super.open(ready)
  }

  override def close(closed: Promise[Closed]): Unit = {
    logger.debug("sink <{}> closing producer instance", getName)
    aliyunSLSProducer.close()

    super.close(closed)
  }
}

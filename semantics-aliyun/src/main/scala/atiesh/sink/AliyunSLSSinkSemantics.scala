/*
 * Copyright (C) Hao Feng
 */

package atiesh.sink

// scala
import scala.util.{ Try, Success, Failure }
import scala.concurrent.{ Promise, Future }
import scala.collection.JavaConverters._
// aliyun
import com.aliyun.openservices.log.common.LogItem
import com.aliyun.openservices.aliyun.log.producer.{ ProjectConfig, ProjectConfigs, ProducerConfig }
import com.aliyun.openservices.aliyun.log.producer.{ LogProducer, Result, Callback }
// internal
import atiesh.event.Event
import atiesh.statement.{ Ready, Closed }
import atiesh.utils.{ Configuration, Logging }

object AliyunSLSSinkSemantics {
  object AliyunSLSSinkSemanticsOpts {
    val OPT_SLS_PRODUCER_ACCESS_KEY_ID = "sls-access-key-id"
    val OPT_SLS_PRODUCER_ACCESS_KEY_SECRET = "sls-access-key-secret"
    val OPT_SLS_PRODUCER_ENDPOINT = "sls-endpoint"
    val OPT_SLS_PRODUCER_PROJEST_NAME = "sls-project-name"
    val OPT_SLS_PRODUCER_LOGSTORE_NAME = "sls-logstore-name"
  }

  case class ProduceRecord(project: String, logstore: String, item: LogItem,
                           topic: Option[String], source: Option[String], shardHash: Option[String])
  object ProduceRecord {
    def apply(project: String, logstore: String, item: LogItem): ProduceRecord = {
      ProduceRecord(project, logstore, item, None, None, None)
    }
  }

  case class ProduceRecords(project: String, logstore: String, items: List[LogItem],
                           topic: Option[String], source: Option[String], shardHash: Option[String])
  object ProduceRecords {
    def apply(project: String, logstore: String, items: List[LogItem]): ProduceRecords = {
      ProduceRecords(project, logstore, items, None, None, None)
    }
  }
}

trait AliyunSLSSinkSemantics extends SinkSemantics with Logging { this: Sink =>
  import AliyunSLSSinkSemantics._

  private var producer: LogProducer = _
  def createProducer(name: String, cfg: Configuration): LogProducer = {
    val accessKeyId = cfg.getString(AliyunSLSSinkSemanticsOpts.OPT_SLS_PRODUCER_ACCESS_KEY_ID)
    val accessKeySecret = cfg.getString(AliyunSLSSinkSemanticsOpts.OPT_SLS_PRODUCER_ACCESS_KEY_SECRET)
    val endpoint = cfg.getString(AliyunSLSSinkSemanticsOpts.OPT_SLS_PRODUCER_ENDPOINT)
    val projectName = cfg.getString(AliyunSLSSinkSemanticsOpts.OPT_SLS_PRODUCER_PROJEST_NAME)
    val logstoreName = cfg.getString(AliyunSLSSinkSemanticsOpts.OPT_SLS_PRODUCER_LOGSTORE_NAME)

    val pcf = new ProjectConfigs()
    pcf.put(new ProjectConfig(projectName, endpoint, accessKeyId, accessKeySecret))

    val producer = new LogProducer(new ProducerConfig(pcf))
    logger.debug("sink <{}> producer created", name)

    producer
  }

  def createProducerCB(p: Promise[Result]): Callback = {
    new Callback {
      override def onCompletion(result: Result) {
        if (result.isSuccessful) {
          p.success(result)
        } else {
          p.failure(new AliyunSLSSinkException(result.getReservedAttempts.asScala.toList))
        }
      }
    }
  }

  def send(event: Event)
          (parser: Event => Try[ProduceRecord]): Future[Result] = {
    val promise = Promise[Result]
    val callback = createProducerCB(promise)

    val result = parser(event) match {
      case Success(record) =>
        record match {
          case ProduceRecord(proj, store, item, None, None, None) =>
            Try { producer.send(proj, store, item, callback) }
          case ProduceRecord(proj, store, item, Some(topic), Some(source), None) =>
            Try { producer.send(proj, store, topic, source, item, callback) }
          case ProduceRecord(proj, store, item, Some(topic), Some(source), Some(hash)) =>
            Try { producer.send(proj, store, topic, source, hash, item, callback) }
          case _ =>
            Try { throw new SinkInvalidEventException(
              "bad event parser response, cannot invoke send, the parser should return a " +
              "ProduceRecord instance which contains <project, logstore, item> or " +
              "<project, logstore, item, topic source> or " +
              "<project, logstore, item, topic, source, shardhash>") }
          }
      case Failure(exc) =>
        Try { throw exc }
    }

    result match {
      case Success(_)   => promise.future
      case Failure(exc) => Future.failed[Result](exc)
    }
  }

  def send(events: List[Event])
          (parser: List[Event] => Try[ProduceRecords]): Future[Result] = {
    val promise = Promise[Result]
    val callback = createProducerCB(promise)

    val result = parser(events) match {
      case Success(record) =>
        record match {
          case ProduceRecords(proj, store, items, None, None, None) =>
            Try { producer.send(proj, store, items.asJava, callback) }
          case ProduceRecords(proj, store, items, Some(topic), Some(source), None) =>
            Try { producer.send(proj, store, topic, source, items.asJava, callback) }
          case ProduceRecords(proj, store, items, Some(topic), Some(source), Some(hash)) =>
            Try { producer.send(proj, store, topic, source, hash, items.asJava, callback) }
          case _ =>
            Try { throw new SinkInvalidEventException(
              "bad event parser response, cannot invoke send, the parser should return a " +
              "ProduceRecords instance which contains <project, logstore, items> or " +
              "<project, logstore, items, topic source> or " +
              "<project, logstore, items, topic, source, shardhash>") }
        }
      case Failure(exc) =>
        Try { throw exc }
    }

    result match {
      case Success(_)   => promise.future
      case Failure(exc) => Future.failed[Result](exc)
    }
  }

  override def start(ready: Promise[Ready]): Unit = {
    val cfg = getConfiguration
    producer = createProducer(getName, cfg)

    super.start(ready)
  }

  override def stop(closed: Promise[Closed]): Unit = {
    logger.debug("sink <{}> closing producer instance", getName)
    producer.close()

    super.stop(closed)
  }
}

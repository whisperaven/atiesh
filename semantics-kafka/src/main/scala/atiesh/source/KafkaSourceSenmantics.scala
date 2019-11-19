/*
 * Copyright (C) Hao Feng
 */

package atiesh.source

// java
import java.util.Properties
import java.util.{ Map => JMap, Collection => JCollection }
import java.util.concurrent.atomic.AtomicLong
// scala
import scala.collection.mutable
import scala.util.{ Success, Failure }
import scala.concurrent.{ ExecutionContext, Promise, Future, Await }
import scala.concurrent.duration._
import scala.collection.JavaConverters._
// akka
import akka.actor.ActorSystem
// kafka
import org.apache.kafka.clients.consumer.{ KafkaConsumer, ConsumerRecords,
                                           OffsetAndMetadata,
                                           OffsetCommitCallback,
                                           ConsumerRebalanceListener }
import org.apache.kafka.common.TopicPartition
// internal
import atiesh.event.{ Event, Empty, SimpleEvent }
import atiesh.statement.{ Ready, Closed }
import atiesh.utils.{ Configuration, Logging }

object KafkaSourceSemantics extends Logging {
  object KafkaSourceSemanticsHeadersName {
    val TOPIC     = "kafkaTopic"
    val PARTITION = "kafkaPartition"
  }

  object KafkaSourceSemanticsOpts {
    val OPT_CONSUMER_PROPERTIES_SECTION = "kafka-properties"

    val OPT_AKKA_DISPATCHER = "kafka-akka-dispatcher"
    val DEF_AKKA_DISPATCHER = "akka.actor.default-dispatcher"

    val OPT_CONSUMER_TOPICS = "topics"
    val DEF_CONSUMER_TOPICS = List[String]()
    val OPT_CONSUMER_POLL_TIMEOUT = "poll-timeout"
    val DEF_CONSUMER_POLL_TIMEOUT = FiniteDuration(1000, MILLISECONDS)
    val OPT_CONSUMER_COMMIT_INTERVAL_POLLS = "commit-polls-interval"
    val DEF_CONSUMER_COMMIT_INTERVAL_POLLS = 0

    val OPT_CONSUMER_SEEK_BEGINNING = "seek-beginning"
    val DEF_CONSUMER_SEEK_BEGINNING = false
    val OPT_CONSUMER_SEEK_END = "seek-end"
    val DEF_CONSUMER_SEEK_END = false
  }

  def commitOffset(owner: String, consumer: KafkaConsumer[_, _]): Unit =
    commitOffset(owner, consumer, None)

  def commitOffset(owner: String,
                   consumer: KafkaConsumer[_, _],
                   commitCB: Option[OffsetCommitCallback]): Unit =
    try {
      commitCB match {
        case None =>
          consumer.commitSync()
          logger.debug("source <{}> consumer offset committed", owner)
        case Some(cb) =>
          consumer.commitAsync(cb)
          logger.debug("source <{}> consumer offset async commit issued", owner)
      }
    } catch {
      case exc: Throwable =>
        logger.warn(s"source <${owner}> consumer offset commit failed, " +
                     s"got commit exception", exc)
    }

  def createCommitCallback(
    p: Promise[Map[TopicPartition, OffsetAndMetadata]]): OffsetCommitCallback =
    new OffsetCommitCallback {
      override def onComplete(offsets: JMap[TopicPartition, OffsetAndMetadata],
                              exception: Exception): Unit = {
        if (exception == null) {
          p.success(offsets.asScala.toMap)
        } else {
          p.failure(exception)
        }
      }
    }
}

trait KafkaSourceSemantics
  extends SourceSemantics
  with KafkaSourceMetrics
  with Logging { this: Source =>
  import KafkaSourceSemantics.{ KafkaSourceSemanticsOpts => Opts, _ }

  @volatile final private var kafkaConsumer: KafkaConsumer[String, String] = _
  @volatile final private var kafkaPollTimeout: FiniteDuration = _

  @volatile final private var kafkaDispatcher: String = _
  @volatile final private var kafkaExecutionContext: ExecutionContext = _

  final private val kafkaPollsCount: AtomicLong = new AtomicLong(0)
  @volatile final private var kafkaCommitIntervalPolls: Long = 0

  type CommitSlot = Option[Future[Map[TopicPartition, OffsetAndMetadata]]]
  @volatile final private var commitSlot: CommitSlot = None

  final def getKafkaDispatcher: String = kafkaDispatcher
  final def getKafkaExecutionContext: ExecutionContext = kafkaExecutionContext

  override def bootstrap()(implicit system: ActorSystem): Unit = {
    super.bootstrap()

    kafkaDispatcher = getConfiguration.getString(Opts.OPT_AKKA_DISPATCHER,
                                                 Opts.DEF_AKKA_DISPATCHER)
    kafkaExecutionContext = system.dispatchers.lookup(kafkaDispatcher)
  }

  import KafkaSourceSemanticsHeadersName._
  def mainCycle(): List[Event] = {
    val records =
      try {
        logger.debug("source <{}> cycle start, polling from kafka", getName)
        kafkaConsumer.poll(kafkaPollTimeout.toMillis)
      } catch {
        case exc: Throwable =>
          logger.error("got unexpected exception while " +
                       "polling from kafka broker", exc)
          ConsumerRecords.empty(): ConsumerRecords[String, String]
      }
    kafkaConsumer.assignment().asScala.foreach(tp => {
      try {
        metricsKafkaSourceTopicPartitionOffsetGauge(tp.topic, tp.partition)
          .update(kafkaConsumer.position(tp))
      } catch {
        case exc: Throwable =>
          logger.warn(s"source <${getName}> failed to fetch offset " +
                      s"position from consumer instance for partition " +
                      s"<${tp.partition}> of topic <${tp.topic}>", exc)
      }
    })
    kafkaPollsCount.incrementAndGet()

    if (!records.isEmpty()) {
      records.asScala.foldLeft(List[Event]())((es, r) => {
        if (r.value != null) {
          SimpleEvent(
            r.value,
            Map[String, String](TOPIC -> r.topic(),
                                PARTITION -> r.partition().toString)) :: es
        } else {
          logger.warn("got empty record from kafka, ignore and process next")
          es
        }
      })
    } else List[Event]()
  }

  def doneCycle(): Unit =
    if (kafkaCommitIntervalPolls != 0 &&
        kafkaPollsCount.get % kafkaCommitIntervalPolls == 0) {
      commitSlot.filter(!_.isCompleted)
        .map(f => {
          logger.info("waiting for pending async commit @ <{}> " +
                      "to complate", f.hashCode.toHexString)
          Await.ready(f, Duration.Inf)
        })

      val p = Promise[Map[TopicPartition, OffsetAndMetadata]]()
      p.future.onComplete({
        case Success(_) =>
          metricsKafkaSourceCommitSuccessCounter.increment()
          metricsKafkaSourceComponentCommitSuccessCounter.increment()
          logger.info("source <{}> consumer offset committed", getName)
        case Failure(exc) =>
          metricsKafkaSourceCommitFailedCounter.increment()
          metricsKafkaSourceComponentCommitFailedCounter.increment()
          logger.warn(s"source <${getName}> consumer offset commit failed, " +
                      s"got commit exception", exc)
      })(kafkaExecutionContext)

      commitOffset(getName, kafkaConsumer, Some(createCommitCallback(p)))
      commitSlot = Some(p.future)
    }

  final def createConsumer(
    ccf: Option[Configuration],
    topics: List[String]): KafkaConsumer[String, String] = {
    val props = new Properties()
    ccf match {
      case Some(c) =>
        for ((k, v) <- c.entrySet) {
          props.put(k, v)
        }
      case None =>
        throw new SourceInitializeException(
          "can not initialize kafka source semantics, " +
          "missing consumer properties")
    }

    val consumer = new KafkaConsumer[String, String](props)
    val listener = new ConsumerRebalanceListener {
      def onPartitionsRevoked(tps: JCollection[TopicPartition]): Unit = {
        logger.info("consumer rebalancing, commit partitions <{}>",
                    tps.toString)
        commitOffset(getName, consumer)

        metricsKafkaSourceRebalanceCounter.increment()
        metricsKafkaSourceComponentRebalanceCounter.increment()

        tps.asScala.foreach(tp => {
          metricsKafkaSourceRemoveTopicPartitionOffsetGauge(tp.topic,
                                                            tp.partition)
        })
      }
      def onPartitionsAssigned(tps: JCollection[TopicPartition]): Unit = {
        logger.info("consumer rebalancing, assigned partitions <{}>",
                    tps.toString)
      }
    }
    consumer.subscribe(topics.asJava, listener)

    logger.debug("source <{}> consumer created and subscribe topic(s): {}",
                 getName, topics)
    consumer
  }

  override def open(ready: Promise[Ready]): Unit = {
    val cfg = getConfiguration
    val topics = cfg.getStringList(Opts.OPT_CONSUMER_TOPICS,
                                   Opts.DEF_CONSUMER_TOPICS)

    logger.debug("source <{}> initialize kafka consomer instances", getName)

    val seekToBeginning = cfg.getBoolean(Opts.OPT_CONSUMER_SEEK_BEGINNING,
                                         Opts.DEF_CONSUMER_SEEK_BEGINNING)
    val seekToEnd       = cfg.getBoolean(Opts.OPT_CONSUMER_SEEK_END,
                                         Opts.DEF_CONSUMER_SEEK_END)
    if (seekToBeginning && seekToEnd) {
      throw new SourceInitializeException(
        s"cannot initialize kafka consumer with both seek to beginning " +
        s"and end, you may want change the setting of " +
        s"<${Opts.OPT_CONSUMER_SEEK_BEGINNING}> or " +
        s"<${Opts.OPT_CONSUMER_SEEK_END}> to false")
    }

    val ccf = cfg.getSection(Opts.OPT_CONSUMER_PROPERTIES_SECTION)
    kafkaConsumer = createConsumer(ccf, topics)

    kafkaPollTimeout = cfg.getDuration(Opts.OPT_CONSUMER_POLL_TIMEOUT,
                                       Opts.DEF_CONSUMER_POLL_TIMEOUT)
    kafkaCommitIntervalPolls = cfg.getLong(
      Opts.OPT_CONSUMER_COMMIT_INTERVAL_POLLS,
      Opts.DEF_CONSUMER_COMMIT_INTERVAL_POLLS)

    if (seekToBeginning || seekToEnd) {
      logger.debug("source <{}> seeking kafka consumer offsets")
      val tps = kafkaConsumer.assignment()
      if (seekToBeginning) {
        logger.debug("source <{}> seeking kafka consumer offsets to beginning")
        kafkaConsumer.seekToBeginning(tps)
      } else {
        logger.debug("source <{}> seeking kafka consumer offsets to end")
        kafkaConsumer.seekToEnd(tps)
      }
    }

    super.open(ready)
  }

  override def close(closed: Promise[Closed]): Unit = {
    commitOffset(getName, kafkaConsumer)

    kafkaConsumer.close()
    logger.debug("source <{}> consumer instance closed", getName)

    super.close(closed)
  }
}

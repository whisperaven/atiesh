/*
 * Copyright (C) Hao Feng
 */

package atiesh.source

// java
import java.nio.charset.{ Charset, StandardCharsets, CodingErrorAction }
import java.nio.file.{ Path, Paths, Files, FileSystems,
                       WatchKey, WatchService, WatchEvent,
                       StandardWatchEventKinds }
import java.util.concurrent.TimeUnit
// scala
import scala.collection.{ mutable, Iterator }
import scala.io.{ Codec, BufferedSource }
import scala.util.{ Try, Success, Failure }
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Promise }
import scala.collection.JavaConverters._
// akka
import akka.actor.ActorSystem
// internal 
import atiesh.event.{ Event, SimpleEvent }
import atiesh.statement.{ Ready, Closed }
import atiesh.utils.{ Configuration, Logging }

object DirectoryWatchSourceSemantics {
  object DirectoryWatchSourceSemanticsOpts {
    val OPT_WATCH_DIRECTORY    = "watch-directory"
    val OPT_WATCH_POLL_TIMEOUT = "watch-poll-timeout"
    val DEF_WATCH_POLL_TIMEOUT = FiniteDuration(1000, MILLISECONDS)

    val OPT_CYCLE_MAX_LINES = "cycle-max-lines"
    val DEF_CYCLE_MAX_LINES = 1500

    val OPT_FILE_CHARSET = "file-charset"
    val DEF_FILE_CHARSET = StandardCharsets.UTF_8.name()

    val OPT_ENABLE_FILEHEADERS = "enable-file-headers"
    val DEF_ENABLE_FILEHEADERS = false

    val OPT_AKKA_DISPATCHER = "dirwatch-akka-dispatcher"
    val DEF_AKKA_DISPATCHER = "akka.actor.default-dispatcher"
  }

  object DirectoryWatchSourceSemanticsHeaders {
    val FILENAME = "fn"
    val OFFSET   = "off"
  }
  val emptyHeaders = Map[String, String]()

  case class InputLines(path: String,
                        stream: BufferedSource,
                        lines: Iterator[String],
                        offset: Int)
}

trait DirectoryWatchSourceSemantics
  extends SourceSemantics
  with DirectoryWatchSourceMetrics
  with Logging { this: Source =>
  import DirectoryWatchSourceSemantics.{
         DirectoryWatchSourceSemanticsOpts => Opts,
         DirectoryWatchSourceSemanticsHeaders => DWHeaders, _ }

  final private[this] var directoryWatchDispatcher: String = _
  final private[this] var directoryWatchExecutionContext: ExecutionContext = _

  final private[this] var directoryWatchService: WatchService = _
  final private[this] var directoryWatchKey: WatchKey = _
  final private[this] var directoryWatchPath: Path = _

  final private[this] var directoryWatchPollTimeout: FiniteDuration = _
  final private[this] var directoryWatchMaxLines: Long = 0
  final private[this] var directoryWatchFileCharsetName: String = _
  final private[this] var directoryWatchFileCodec: Codec = _
  final private[this] var directoryWatchEnableFileHeaders: Boolean = _

  final def getDirectoryWatchPath: Path = directoryWatchPath
  final def getDirectoryWatchDispatcher: String = directoryWatchDispatcher
  final def getDirectoryWatchExecutionContext: ExecutionContext =
    directoryWatchExecutionContext

  override def bootstrap()(implicit system: ActorSystem): Unit = {
    super.bootstrap()

    directoryWatchService = FileSystems.getDefault().newWatchService()

    directoryWatchDispatcher =
      getConfiguration.getString(Opts.OPT_AKKA_DISPATCHER,
                                 Opts.DEF_AKKA_DISPATCHER)
    directoryWatchExecutionContext =
      system.dispatchers.lookup(directoryWatchDispatcher)
  }

  final private[this] val incomings = mutable.Queue[(Path, Int)]()
  final private[this] def openNextFile(): Option[InputLines] = {
    if (incomings.isEmpty) {
      pollFiles() match {
        case Success(paths) =>
          paths.foreach(path => {
            incomings.enqueue((path, 0))

            metricsDirectoryWatchSourceInQueueItemsGauge.increment()
            metricsDirectoryWatchSourceComponentInQueueItemsGauge.increment()
          })

        case Failure(exc)   =>
          logger.warn(s"source <${getName}> got error while polling " +
                      s"from directory watch service, ignore and " +
                      s"standby for next source cycle", exc)
      }
    }

    incomings.headOption.flatMap(_ => {
      val (path, offset) = incomings.dequeue()

      metricsDirectoryWatchSourceInQueueItemsGauge.decrement()
      metricsDirectoryWatchSourceComponentInQueueItemsGauge.decrement()

      try {
        val stream = scala.io.Source.fromFile(path.toString)(
                                              directoryWatchFileCodec)
        val lines = stream.getLines().drop(offset)

        metricsDirectoryWatchSourceOpenFilesCounter.increment()
        metricsDirectoryWatchSourceComponentOpenFilesCounter.increment()

        Some(InputLines(path.toString, stream, lines, offset))
      } catch {
        case exc: Throwable =>
          logger.warn(s"source <${getName}> got error while tring open " +
                      s"new file <${path.toString}>, the file may deleted " +
                      s"by user implement or some bad guy, ignore and " +
                      s"standby for next source cycle", exc)
          None
      }
    })
  }

  final private[this] def pollFiles(): Try[List[Path]] = Try {
    val wk = directoryWatchService.poll(directoryWatchPollTimeout.toMillis,
                                        TimeUnit.MILLISECONDS)
    if (wk == null) {
      List[Path]()
    } else {
      val wes = wk.pollEvents().asScala
      val paths = wes.foldLeft(List[Path]())((ps, we) => {
        val p = 
          if (we.kind() == StandardWatchEventKinds.OVERFLOW) {
            logger.warn("source <{}> got overflow event from directories, " +
                        "ignore and continue to next one", getName)
            None
          } else {
            val path =
              directoryWatchPath.resolve(
                we.asInstanceOf[WatchEvent[Path]].context())
            if (!path.toFile().isFile() || !path.toFile().canWrite()) {
              logger.error("source <{}> got new path <{}> with either " +
                           "unexpected file type or don't have privileges " +
                           "to read & write & delete it",
                           getName, path.toString)
              None
            } else {
              Some(path)
            }
          }
        p.map(_ :: ps).getOrElse(ps)
      })
      if (!wk.reset()) {
        logger.error("source <{}> cannot reseet watch key, the key is " +
                     "no longer valid, this should never happen, " +
                     "the watched directory is now inaccessible " +
                     "(may be deleted by somebody)", getName)
      }
      paths.filter(path => {
        val skip = skipFile(path)
        if (skip) {
          logger.debug(
            "source <{}> skip file <{}> by user implement decision",
            getName, path.toString)
        }
        !skip
      })
    }
  }

  final private[this] var inputLines: Option[InputLines] = None
  def mainCycle(): List[Event] = {
    val input = inputLines match {
      case inputs @ Some(lines) =>
        logger.debug("source <{}> reading file <{}> @ offset <{}>",
                     getName, lines.path, lines.offset)
        inputs
      case None => 
        logger.debug("source <{}> trying open new file", getName)
        openNextFile()
    }

    input match {
      case Some(InputLines(path, stream, lines, offset)) =>
        var reads = 0
        val buffer = mutable.ArrayBuffer[Event]()
        while (lines.hasNext && reads < directoryWatchMaxLines) {
          val headers = if (directoryWatchEnableFileHeaders) {
            Map[String, String]((DWHeaders.FILENAME -> path),
                                (DWHeaders.OFFSET -> (offset + reads).toString))
          } else emptyHeaders
          buffer.append(SimpleEvent(lines.next, headers))
          reads += 1
        }

        inputLines = if (lines.hasNext) { /* update input context */
          Some(InputLines(path, stream, lines, offset + reads))
        } else {
          logger.debug(
            "source <{}> closing file <{}>, total read <{}> lines",
            getName, path, offset + reads)
          stream.close()

          try { doneFile(Paths.get(path)) } catch {
            case exc: Throwable =>
              logger.error(s"source <${getName}> got unexpected exception " +
                           s"inside user define hook <doneFile> while " +
                           s"dealing with file <${path}>, you may use a " +
                           s"filesystem source with wrong or buggy " +
                           s"implementation", exc)
          }

          metricsDirectoryWatchSourceDoneFilesCounter.increment()
          metricsDirectoryWatchSourceComponentDoneFilesCounter.increment()

          None
        }
        buffer.toList
      case None =>
        logger.debug(
          "source <{}> got no new file, schedule next cycle", getName)
        List[Event]()
    }
  }

  final protected[this] def listFiles(path: Path): Try[List[Path]] = Try {
    Files.list(path).iterator()
      .asScala.foldLeft(List[Path]())((ps, p) => { p :: ps })
  }

  final protected[this] def pushFiles(files: List[(Path, Int)]): Unit =
    files.foreach({
      case (path, offset) =>
        incomings.enqueue((path -> offset))

        metricsDirectoryWatchSourceInQueueItemsGauge.increment()
        metricsDirectoryWatchSourceComponentInQueueItemsGauge.increment()
    })

  final protected[this] def getLinesContext: Option[(Path, Int)] =
    inputLines.map(i => { (Paths.get(i.path), i.offset) })

  /**
   * Thread safe method, user defined hook for files which got EOF, note
   * that the file stream already colsed before invoke this method.
   */
  def doneFile(path: Path): Unit

  /**
   * Thread safe method, user defined hook for skip the new file
   * returned by the <WatchService> of the watched directory.
   */
  def skipFile(path: Path): Boolean

  override def open(ready: Promise[Ready]): Unit = {
    val cfg = getConfiguration
    val directory = cfg.getString(Opts.OPT_WATCH_DIRECTORY)

    directoryWatchMaxLines = cfg.getLong(Opts.OPT_CYCLE_MAX_LINES,
                                         Opts.DEF_CYCLE_MAX_LINES)
    directoryWatchPollTimeout = cfg.getDuration(Opts.OPT_WATCH_POLL_TIMEOUT,
                                                Opts.DEF_WATCH_POLL_TIMEOUT)
    directoryWatchFileCharsetName = cfg.getString(Opts.OPT_FILE_CHARSET,
                                                  Opts.DEF_FILE_CHARSET)
    directoryWatchEnableFileHeaders =
      cfg.getBoolean(Opts.OPT_ENABLE_FILEHEADERS, Opts.DEF_ENABLE_FILEHEADERS)

    val fs = FileSystems.getDefault()
    directoryWatchPath = fs.getPath(directory)

    if (!directoryWatchPath.isAbsolute()) {
      throw new SourceInitializeException(
        s"cannot initialize directory watch services for directory " +
        s"${directory}, it should be absolute path")
    }
    if (!directoryWatchPath.toFile().isDirectory() ||
        !directoryWatchPath.toFile().canWrite()) {
      throw new SourceInitializeException(
        s"cannot initialize directory watch services for directory " +
        s"${directory}, it is not a directory or we don't have privileges " +
        s"to read & write into it")
    }

    logger.debug("source <{}> start watching directory: <{}>",
                 getName, directory)

    try {
      directoryWatchKey =
        directoryWatchPath.register(directoryWatchService,
                                    StandardWatchEventKinds.ENTRY_CREATE)
    } catch {
      case exc: Throwable =>
        throw new SourceInitializeException(
          s"cannot initialize filesystem directory watch source, " +
          s"got unexpected exception while trying watch directory " +
          s"${directoryWatchPath.toFile.getName}, abort initialize", exc)
    }

    try {
      Charset.forName(directoryWatchFileCharsetName)
    } catch {
      case exc: Throwable =>
        throw new SourceInitializeException(
          s"cannot initialize directory watch source with given " +
          s"charset <${directoryWatchFileCharsetName}>, which is illegal " +
          s"or unsupported charset name, you may want change the setting " +
          s"of <${Opts.OPT_FILE_CHARSET}> to a valid one", exc)
    }
    directoryWatchFileCodec = Codec(directoryWatchFileCharsetName)
                                .onMalformedInput(CodingErrorAction.IGNORE)

    super.open(ready)
  }

  override def close(closed: Promise[Closed]): Unit = {
    inputLines.map(input => {
      logger.debug(
        "source <{}> closing current stream <{}>", getName, input.path)
      input.stream.close()
      logger.info(
        "source <{}> closed current stream <{}>", getName, input.path)
    })
    super.close(closed)
  }
}

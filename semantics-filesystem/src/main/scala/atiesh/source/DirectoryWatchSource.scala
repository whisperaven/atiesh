/*
 * Copyright (C) Hao Feng
 */

package atiesh.source

// java
import java.nio.file.Path
// scala
import scala.io.{ Codec, Source => IOSource }
import scala.collection.Iterator
// internal
import atiesh.interceptor.Interceptor
import atiesh.sink.Sink
import atiesh.utils.{ Configuration, Logging }

class DirectoryWatchSource(name: String,
                           dispatcher: String,
                           cfg: Configuration,
                           interceptors: List[Interceptor],
                           sinks: List[Sink],
                           strategy: String)
  extends AtieshSource(name, dispatcher, cfg, interceptors, sinks, strategy)
  with ActiveSourceSemantics
  with DirectoryWatchSourceSemantics
  with Logging {
  import DirectoryWatchSourceSemantics.scalaFileOpener

  def openFile(path: String,
               codec: Codec): IOSource =
    scalaFileOpener(path, codec)

  def getLinesIterator(stream: IOSource,
                       offset: Int): Iterator[String] =
    stream.getLines().drop(offset)

  def skipFile(path: Path): Boolean = false

  def doneFile(path: Path): Unit = logger.debug("directory watch source " +
                                                "<{}> handle EOF for file: " +
                                                "<{}>", getName, path.toString)

  def doneCycle(): Unit = logger.debug("directory watch source <{}> cycle " +
                                       "done, schedule next cycle", getName)

  def shutdown(): Unit = logger.info("shutting down directory watch " +
                                     "source <{}>", getName)

  def startup(): Unit = logger.info("starting directory watch " +
                                    "source <{}>", getName)
}

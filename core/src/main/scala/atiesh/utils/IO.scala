/*
 * Copyright (C) Hao Feng
 */

package atiesh.utils

// java
import java.io.{ File => JFile, InputStream, FileInputStream,
                 InputStreamReader, BufferedReader }
// scala
import scala.io.{ Codec, Source }
import scala.collection.{ Iterator, AbstractIterator }
import scala.collection.mutable.StringBuilder

object IO {
  val DEFAULT_EXPECTED_LINE_LENGTH = 80
  val DEFAULT_LINE_SEPARATOR = '\n'
  val DEFAULT_BUF_SIZE = 8192
  val DEFAULT_MAX_LINE_LENGTH = 512 * 1024 * 1024 /* assumed 2 bytes pre char */

  def createIOSource(rawStream: InputStream,
                     bufferSize: Int,
                     resetFn: () => IOSource,
                     closeFn: () => Unit)(implicit codec: Codec): IOSource =
    new IOSource(rawStream, bufferSize)(codec)
      .withReset(resetFn)
      .withClose(closeFn)

  def fromFile(path: JFile,
               bufferSize: Int)(implicit codec: Codec): IOSource = {
    val rawInputStream = new FileInputStream(path)
    createIOSource(rawInputStream,
                   bufferSize, 
                   () => fromFile(path, bufferSize)(codec),
                   () => rawInputStream.close())(codec)
  }

  def fromFile(path: JFile)(implicit codec: Codec): IOSource =
    fromFile(path, DEFAULT_BUF_SIZE)(codec)

  def fromFile(name: String,
               bufferSize: Int)(implicit codec: Codec): IOSource = {
    fromFile(new JFile(name), bufferSize)
  }

  def fromFile(name: String)(implicit codec: Codec): IOSource = {
    fromFile(new JFile(name))
  }
}

class IOSource(rawStream: InputStream,
               bufferSize: Int)(implicit codec: Codec) extends Source {
  def this(rawStream: InputStream)(implicit codec: Codec) =
    this(rawStream, IO.DEFAULT_BUF_SIZE)(codec)

  final private[this] val charReader =
    new InputStreamReader(rawStream, codec.decoder)
  final private[this] val bufferedCharReader =
    new BufferedReader(charReader, bufferSize)

  /* scala io.Source iter */
  override val iter = Iterator.continually(codec.wrap(bufferedCharReader.read()))
                              .takeWhile(_ != -1)
                              .map(_.toChar)

  class LineIterator(lineSeparator: Char,
                     lineExpectedLength: Int,
                     lineMaxLength: Int)
    extends AbstractIterator[String] with Iterator[String] {
    def this() = this(IO.DEFAULT_LINE_SEPARATOR,
                      IO.DEFAULT_EXPECTED_LINE_LENGTH,
                      IO.DEFAULT_MAX_LINE_LENGTH)
 
    var nextLine: String = null
    private def readLine(): String =
      iter.takeWhile(_ != lineSeparator)
          .foldLeft(
            new StringBuilder(IO.DEFAULT_EXPECTED_LINE_LENGTH))((sb, c) => {
              if (sb.length() < lineMaxLength) sb.append(c)
              sb
            }).toString

    override def hasNext(): Boolean = {
      if (nextLine == null) {
        nextLine = readLine()
      }
      nextLine != null
    }
    
    override def next(): String =
      if (hasNext()) {
        try nextLine
        finally nextLine = null
      } else {
        throw new NoSuchElementException("next on empty iterator")
      }
  }

  def getLines(lineSeparator: Char,
               lineExpectedLength: Int,
               lineMaxLength: Int): Iterator[String] =
    new LineIterator(lineSeparator, lineExpectedLength, lineMaxLength)
  override def getLines(): Iterator[String] = new LineIterator()
}

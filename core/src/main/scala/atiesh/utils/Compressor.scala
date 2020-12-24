/*
 * Copyright (C) Hao Feng
 */

package atiesh.utils

// java
import java.io.{ ByteArrayOutputStream, ByteArrayInputStream }
import java.util.zip.{ GZIPOutputStream, GZIPInputStream }
// scala
import scala.io.Source.DefaultBufSize
import scala.collection.mutable
import scala.util.Try

object Compressor {
  def gzipCompress(input: Array[Byte]): Array[Byte] = {
    val compressed = new ByteArrayOutputStream(input.length)
    val gzip = new GZIPOutputStream(compressed)

    gzip.write(input)
    gzip.close()
    compressed.close()

    compressed.toByteArray
  }

  def gzipDecompress(input: Array[Byte]): Try[Array[Byte]] = Try {
    val compressed = new ByteArrayInputStream(input)
    val bytes  = new GZIPInputStream(compressed)
    val buffer = new Array[Byte](DefaultBufSize)
    val output = new mutable.ArrayBuffer[Byte]()

    /**
     * Not use scala.io.Source to avoid Codec here, just return raw bytes.
     */
    while (bytes.available() == 1) {
      val c = bytes.read(buffer, 0, buffer.length)
      if (c > 0) {
        output.appendAll(buffer.take(c))
      }
    }
    output.toArray
  }
}

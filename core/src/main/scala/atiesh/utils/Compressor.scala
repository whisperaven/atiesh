/*
 * Copyright (C) Hao Feng
 */

package atiesh.utils

// java
import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream

object Compressor {
  def gzipCompress(input: Array[Byte]): Array[Byte] = {
    val compressed = new ByteArrayOutputStream(input.length)
    val gzip = new GZIPOutputStream(compressed)

    gzip.write(input)
    gzip.close()
    compressed.close()

    compressed.toByteArray
  }
}

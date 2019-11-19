/*
 * Copyright (C) Hao Feng
 */

package atiesh.source

class SourceInitializeException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
  def this(message: String) = this(message, null)
  def this(cause: Throwable) = this(null, cause)
  def this() = this(null, null)
}

class SourceCommitFailedException(val message: String,
                                  val causes: List[(String, Throwable)])
  extends RuntimeException(message, causes.headOption.map(_._2).getOrElse(null)) {
  def this(message: String) = this(message, List[(String, Throwable)]())
  def this(cause: Throwable) = this(null, List[(String, Throwable)](("" -> cause)))
  def this(causes: List[(String, Throwable)]) = this(null, causes)
  def this() = this(null, List[(String, Throwable)]())

  def getCauses(): List[(String, Throwable)] = causes
  def getFailedAcks(): List[(String, Throwable)] = getCauses
}

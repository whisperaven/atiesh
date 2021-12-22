/*
 * Copyright (C) Hao Feng
 */

package atiesh.utils

// java
import java.security.GeneralSecurityException

class ConfigParseException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
  def this(message: String) = this(message, null)
  def this(cause: Throwable) = this(null, cause)
  def this() = this(null, null)
}

class UninitializedExtensionException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
  def this(message: String) = this(message, null)
  def this(cause: Throwable) = this(null, cause)
  def this() = this(null, null)
}

class PKIException(message: String, cause: Throwable) extends GeneralSecurityException(message, cause) {
  def this(message: String) = this(message, null)
  def this(cause: Throwable) = this(null, cause)
  def this() = this(null, null)
}

/*
 * Copyright (C) Hao Feng
 */

package atiesh.sink

// aliyun
import com.aliyun.openservices.aliyun.log.producer.Attempt

class AliyunSLSSinkException(message: String, attempts: List[Attempt], cause: Throwable) extends RuntimeException(message, cause) {
  def this(message: String) = this(message, null, null)
  def this(attempts: List[Attempt]) = this(null, attempts, null)
  def this(cause: Throwable) = this(null, null, cause)
  def this() = this(null, null, null)
}

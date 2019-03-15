/*
 * Copyright (C) Hao Feng
 */

package atiesh.statement

// scala
import scala.concurrent.Promise

sealed trait Statement

case object Continue extends Statement

case class Open(ready: Promise[Ready]) extends Statement
case class Ready(component: String)    extends Statement

case class Close(closed: Promise[Closed]) extends Statement
case class Closed(component: String)      extends Statement

case class Commit(tran: Promise[Transaction]) extends Statement
case class Transaction(owner: String)         extends Statement
case class Signal(sig: Int)                   extends Statement

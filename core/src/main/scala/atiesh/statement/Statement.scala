/*
 * Copyright (C) Hao Feng
 */

package atiesh.statement

// scala
import scala.concurrent.Promise
// internal
import atiesh.event.Event

sealed trait Statement

/**
 * Atiesh common component statement.
 */
final case class Open(ready: Promise[Ready])    extends Statement
final case class Ready(owner: String)           extends Statement
final case class Close(closed: Promise[Closed]) extends Statement
final case class Closed(owner: String)          extends Statement

/**
 * Atiesh source component statement.
 */
final case class  Offer(events: List[Event],
                        confirm: Promise[Confirmation]) extends Statement
final case class  Confirmation(owner: String)           extends Statement
final case object Continue                              extends Statement

/**
 * Atiesh sink component statement.
 */
final case class Batch(events: List[Event],
                       aggregatedBy: String)        extends Statement
final case class Commit(committer: String,
                        tran: Promise[Transaction]) extends Statement
final case class Transaction(owner: String)         extends Statement
final case class Signal(sig: Int)                   extends Statement

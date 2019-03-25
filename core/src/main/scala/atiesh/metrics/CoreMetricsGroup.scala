/*
 * Copyright (C) Hao Feng
 */

package atiesh.metrics

object MetricsGroup {
  // sources
  object SourceMetrics {
    val cycleRunsCount     = Metrics.newCounter("Core.Source.CycleRunsCount")
    val eventAcceptedCount = Metrics.newCounter("Core.Source.EventAcceptedCount")
  }

  // interceptors
  object InterceptorMetrics {
    val eventInterceptFailedCount = Metrics.newCounter("Core.Interceptor.EventInterceptFailedCount")
    val eventDiscardedCount       = Metrics.newCounter("Core.Interceptor.EventDiscardedCount")
  }

  // sinks
  object SinkMetrics {
    val eventSubmitCount       = Metrics.newCounter("Core.Sink.EventDrainAttemptCount")
    val signalSendCount        = Metrics.newCounter("Core.Sink.SignalSendAttemptCount")
    val transactionCommitCount = Metrics.newCounter("Core.Sink.TransactionCommitAttemptCount")
    val transactionAckedCount  = Metrics.newCounter("Core.Sink.TransactionAcknowledgedCount")
  }
}

/*
 * Copyright (C) Hao Feng
 */

package atiesh.sink

// java
import com.cloudbees.syslog.sender.{
  TcpSyslogMessageSender,
  UdpSyslogMessageSender,
  AbstractSyslogMessageSender }
import com.cloudbees.syslog.{
  Facility,
  Severity,
  MessageFormat }
// akka
import akka.actor.ActorSystem
// internal
import atiesh.utils.{ Configuration, Logging }

trait SyslogSinkSemantics extends SinkSemantics with Logging { this: Sink =>
  object SyslogSemanticsOpts {
    val OPT_REMOTE_SERVER   = "remote-server"
    val OPT_REMOTE_PORT     = "remote-port"
    val OPT_SYSLOG_HOSTNAME = "syslog-hostname"
    val DEF_SYSLOG_HOSTNAME = "localhost"
    val OPT_SYSLOG_APPNAME  = "syslog-appname"
    val DEF_SYSLOG_APPNAME  = "atiesh"
    val OPT_SYSLOG_FACILITY = "syslog-facility"
    val DEF_SYSLOG_FACILITY = "user"
    val OPT_SYSLOG_SEVERITY = "syslog-severity"
    val DEF_SYSLOG_SEVERITY = "informational"

    val OPT_SYSLOG_IMPL   = "syslog-implemenation"
    val DEF_SYSLOG_IMPL   = SYSLOG_VALID_IMPLEMENTATIONS(1)
    val OPT_MAX_RETRIES      = "max-retries"
    val DEF_MAX_RETRIES: Int = 3
  }
  val SYSLOG_VALID_IMPLEMENTATIONS = Array[String](
    "rfc3164tcp", "rfc3164udp", "rfc3164tls",
    "rfc5425tcp", "rfc5425udp", "rfc5424tls",
    "rfc6587tcp", "rfc6587tls")
  var syslogMessageSender: AbstractSyslogMessageSender = _

  override def bootstrap()(implicit system: ActorSystem): Sink = {
    super.bootstrap()

    val cfg = getConfiguration
    val remoteServer = cfg.getString(SyslogSemanticsOpts.OPT_REMOTE_SERVER)
    val remotePort   = cfg.getInt(SyslogSemanticsOpts.OPT_REMOTE_PORT)

    val pushMaxRetries = cfg.getInt(
      SyslogSemanticsOpts.OPT_MAX_RETRIES,
      SyslogSemanticsOpts.DEF_MAX_RETRIES)

    val syslogImplementation = cfg.getString(
      SyslogSemanticsOpts.OPT_SYSLOG_IMPL,
      SyslogSemanticsOpts.DEF_SYSLOG_IMPL).toLowerCase
    if (!SYSLOG_VALID_IMPLEMENTATIONS.contains(syslogImplementation)) {
      throw new SinkInitializeException(
        s"cannot initialize syslog sink with invalid implemenation <${syslogImplementation}>, " +
        s"should be one of <${SYSLOG_VALID_IMPLEMENTATIONS.mkString(",")}>")
    }

    val syslogHostname = cfg.getString(
      SyslogSemanticsOpts.OPT_SYSLOG_HOSTNAME,
      SyslogSemanticsOpts.DEF_SYSLOG_HOSTNAME)
    val syslogAppname  = cfg.getString(
      SyslogSemanticsOpts.OPT_SYSLOG_APPNAME,
      SyslogSemanticsOpts.DEF_SYSLOG_APPNAME)
    val (syslogFacility, syslogSeverity) = try {
      val facility = Facility.fromLabel(cfg.getString(
        SyslogSemanticsOpts.OPT_SYSLOG_FACILITY,
        SyslogSemanticsOpts.DEF_SYSLOG_FACILITY).toUpperCase)
      val severity = Severity.fromLabel(cfg.getString(
        SyslogSemanticsOpts.OPT_SYSLOG_SEVERITY,
        SyslogSemanticsOpts.DEF_SYSLOG_SEVERITY).toUpperCase)
      (facility, severity)
    } catch {
      case exc: Throwable =>
        throw new SinkInitializeException(
          s"cannot initialize syslog sink, invalid facility or severity given",
          exc)
    }
    val syslogMessageFormat = syslogImplementation match {
      case impl if impl.contains("3164") => MessageFormat.RFC_3164
      case impl if impl.contains("5424") => MessageFormat.RFC_5424
      case impl if impl.contains("5425") => MessageFormat.RFC_5425
      case _ => /* just in case */
        throw new SinkInitializeException(
          s"cannot initialize syslog sink with non-supported message format, " +
          "you may hit a bug")
    }

    syslogMessageSender = syslogImplementation match {
      case "rfc3164tcp" | "rfc5425tcp" | "rfc6587tcp" => 
        val sender = new TcpSyslogMessageSender()
        sender.setSyslogServerHostname(remoteServer)
        sender.setSyslogServerPort(remotePort)
        sender.setMaxRetryCount(pushMaxRetries)
        sender
      case "rfc3164tls" | "rfc5424tls" | "rfc6587tls" =>
        val sender = new TcpSyslogMessageSender()
        sender.setSyslogServerHostname(remoteServer)
        sender.setSyslogServerPort(remotePort)
        sender.setMaxRetryCount(pushMaxRetries)
        sender.setSsl(true)
        sender
      case "rfc3164udp" | "rfc5425udp"                =>
        val sender = new UdpSyslogMessageSender()
        sender.setSyslogServerHostname(remoteServer)
        sender.setSyslogServerPort(remotePort)
        sender
      case _ => /* just in case */
        throw new SinkInitializeException(
          s"cannot initialize syslog sink with non-supported implementation, " +
          "you may hit a bug")
    }
    syslogMessageSender.setDefaultMessageHostname(syslogHostname)
    syslogMessageSender.setDefaultAppName(syslogAppname)
    syslogMessageSender.setDefaultFacility(syslogFacility)
    syslogMessageSender.setDefaultSeverity(syslogSeverity)
    syslogMessageSender.setMessageFormat(syslogMessageFormat)

    this
  }

  def process(sig: Int): Unit = ()

  def sendSyslogMessage(message: String): Unit = syslogMessageSender.sendMessage(message)
}

/*
 * Copyright (C) Hao Feng
 */

import sbt.Credentials
import sbt.Keys.{ credentials, publishTo }

lazy val common = Seq(
  organization := "io.0ops",
  version := "2.2.1",
  scalaVersion := "2.11.12",

  publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository"))),
  credentials += Credentials(Path.userHome / ".sbt" / ".credentials")
)

lazy val distribution = Project(
  id = "atiesh-distribution",
  base = file(".")
).aggregate(core, httputils, kafka, http, syslog)
 .settings(common)

/* atiesh core project */
lazy val dependencies = Seq(
    // configparser
    "com.typesafe"                % "config"              % "1.3.2",
    // kamon
    "io.kamon"                   %% "kamon-core"          % "1.0.0" 
      exclude("org.slf4j", "slf4j-api") exclude("org.slf4j", "slf4j-log4j12")
      exclude("com.typesafe", "config"),
    "io.kamon"                   %% "kamon-system-metrics"% "1.0.0",
    // akka-actor
    "com.typesafe.akka"          %% "akka-actor"          % "2.5.12",
    // logger
    "ch.qos.logback"              % "logback-classic"     % "1.2.3", // scalalogging docs
    "com.typesafe.scala-logging" %% "scala-logging"       % "3.9.0"
)
lazy val core = (project in file("core"))
  .settings(
    common,
    name := "atiesh",
    libraryDependencies ++= dependencies
  )

/* atiesh utils http */
lazy val httputils = (project in file("utils/http"))
  .settings(
    common,
    name := "atiesh-utils-http",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream" % "2.5.12"
        exclude("com.typesafe", "config"),
      "com.typesafe.akka" %% "akka-http" % "10.1.1",
    )
  ).dependsOn(core)

/* atiesh semantics kafka */
lazy val kafka = (project in file("semantics-kafka"))
  .settings(
    common,
    name := "atiesh-semantics-kafka",
    libraryDependencies ++= Seq(
      "org.apache.kafka" %% "kafka" % "1.1.1"
        exclude("org.slf4j", "slf4j-api") exclude("org.slf4j", "slf4j-log4j12")
        exclude("com.typesafe.scala-logging", "scala-logging_2.11")
        exclude("com.typesafe.scala-logging", "scala-logging_2.12")
    )
  ).dependsOn(core)

/* atiesh semantics http */
lazy val http = (project in file("semantics-http"))
  .settings(
    common,
    name := "atiesh-semantics-http",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream" % "2.5.12"
        exclude("com.typesafe", "config"),
      "com.typesafe.akka" %% "akka-http" % "10.1.1",
    )
  ).dependsOn(httputils)

/* atiesh semantics syslog (experiment) */
lazy val syslog = (project in file("semantics-syslog"))
  .settings(
    common,
    name := "atiesh-semantics-syslog",
    libraryDependencies ++= Seq(
      "com.cloudbees" % "syslog-java-client" % "1.1.4"
    )
  ).dependsOn(core)

/*
 * atiesh semantics aliyun,
 *      too much deps with conflicts warns,
 *      we don't build this by default
 *
 * you can build it manual via sbt shell:
 *      sbt> project aliyun
 *      sbt:atiesh-semantics-aliyun> clean
 *      sbt:atiesh-semantics-aliyun> package (or publish)
 */
lazy val aliyun = (project in file("semantics-aliyun"))
  .settings(
    common,
    name := "atiesh-semantics-aliyun",
    libraryDependencies ++= Seq(
      "com.aliyun.openservices" % "aliyun-log-producer" % "0.2.0"
        exclude("org.slf4j", "slf4j-api") exclude("org.slf4j", "slf4j-log4j12")
        exclude("ch.qos.logback", "logback-core") exclude("ch.qos.logback", "logback-classic"),
    )
  ).dependsOn(core)
